/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "replica/IndexJob.h"

// System headers
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ServiceProvider.h"
#include "replica/StopRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IndexJob");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

json IndexJobResult::toJson() const {
    json result;
    for (auto&& workerItr : error) {
        string const& worker = workerItr.first;
        result[worker] = json::object();
        json& workerJson = result[worker];
        for (auto&& chunksItr : workerItr.second) {
            unsigned int chunk = chunksItr.first;
            string const& errorMessage = chunksItr.second;
            workerJson[chunk] = errorMessage;
        }
    }
    return result;
}

string IndexJob::typeName() { return "IndexJob"; }

string IndexJob::toString(Destination destination) {
    switch (destination) {
        case DISCARD:
            return "DISCARD";
        case FILE:
            return "FILE";
        case FOLDER:
            return "FOLDER";
        case TABLE:
            return "TABLE";
    }
    throw range_error(typeName() + "::" + string(__func__) + "  unhandled value of the parameter");
}

IndexJob::Destination IndexJob::fromString(string const& str) {
    if (str == "DISCARD") return Destination::DISCARD;
    if (str == "FILE") return Destination::FILE;
    if (str == "FOLDER") return Destination::FOLDER;
    if (str == "TABLE") return Destination::TABLE;
    throw invalid_argument(typeName() + "::" + string(__func__) + "  input value '" + str +
                           "' doesn't match any known option of the enumerator");
}

IndexJob::Ptr IndexJob::create(string const& databaseName, string const& directorTableName,
                               bool hasTransactions, TransactionId transactionId, bool allWorkers,
                               Destination destination, string const& destinationPath, bool localFile,
                               Controller::Ptr const& controller, string const& parentJobId,
                               CallbackType const& onFinish, int priority) {
    return Ptr(new IndexJob(databaseName, directorTableName, hasTransactions, transactionId, allWorkers,
                            destination, destinationPath, localFile, controller, parentJobId, onFinish,
                            priority));
}

IndexJob::IndexJob(string const& databaseName, string const& directorTableName, bool hasTransactions,
                   TransactionId transactionId, bool allWorkers, Destination destination,
                   string const& destinationPath, bool localFile, Controller::Ptr const& controller,
                   string const& parentJobId, CallbackType const& onFinish, int priority)
        : Job(controller, parentJobId, "INDEX", priority),
          _directorTableName(directorTableName),
          _hasTransactions(hasTransactions),
          _transactionId(transactionId),
          _allWorkers(allWorkers),
          _destination(destination),
          _destinationPath(destinationPath),
          _localFile(localFile),
          _onFinish(onFinish) {
    // Get and verify database status
    try {
        _database = controller->serviceProvider()->config()->databaseInfo(databaseName);
        if (!_database.findTable(directorTableName).isDirector) {
            throw runtime_error(context() + "::" + string(__func__) + " no such director table '" +
                                directorTableName + "' in the database: '" + _database.name + "'.");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, ex.what());
        throw;
    }
}

IndexJob::~IndexJob() { _rollbackTransaction(__func__); }

Job::Progress IndexJob::progress() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    util::Lock lock(_mtx, context() + __func__);
    return Progress{_completeChunks, _totalChunks};
}

IndexJobResult const& IndexJob::getResultData() const {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (state() == State::FINISHED) return _resultData;

    throw logic_error(typeName() + "::" + string(__func__) +
                      "  the method can't be called while the job hasn't finished");
}

list<std::pair<string, string>> IndexJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("directorTable", directorTable());
    result.emplace_back("has_transactions", bool2str(hasTransactions()));
    result.emplace_back("transaction_id", to_string(transactionId()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    result.emplace_back("destination", toString(destination()));
    result.emplace_back("destination_path", destinationPath());
    result.emplace_back("local_file", bool2str(localFile()));
    return result;
}

list<pair<string, string>> IndexJob::persistentLogData() const {
    // Report failed chunks only

    list<pair<string, string>> result;
    for (auto&& workerItr : getResultData().error) {
        auto&& worker = workerItr.first;
        for (auto&& chunkItr : workerItr.second) {
            auto&& chunk = chunkItr.first;
            auto&& error = chunkItr.second;
            if (!error.empty()) {
                result.emplace_back("worker=" + worker + " chunk=" + to_string(chunk), "error=" + error);
            }
        }
    }
    return result;
}

void IndexJob::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // ------------------------
    // Stage I: replica scanner
    // ------------------------

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const workerNames = allWorkers() ? controller()->serviceProvider()->config()->allWorkers()
                                          : controller()->serviceProvider()->config()->workers();

    // Initialize a collection of chunks grouped by workers, in a way which
    // would make an attempt to keep requests equally (as much as that's possible)
    // balanced between the workers.
    //
    // Note, that the algorithm considers a possibility that chunks may have
    // multiple (more than 1) replicas. In this scenario a choice which replica
    // of a particular chunk to use will be based on the number of the chunk's
    // replicas as well as on the total number of chunks per each worker.

    // The first step is to find workers which store replicas of each
    // chunk to be processed by the job.

    bool const unusedAllDatabases = false;  // is still required by the method's contract
    bool const unusedIsPublished = true;    // is still required by the method's contract
    bool const includeFileInfo = false;     // to speed up the query as we don't need file info

    map<unsigned int, list<string>> chunk2workers;
    for (auto&& worker : workerNames) {
        // Scan for chunk replicas at the worker. The algorithm fills the data structure
        // used by the planner algorithm. The scanner has two flavors that depend on the input
        // parameters to the class.
        //   - If a specific transaction was requested then the algorithm will look at
        //     the actual chunk contributions made into the 'director' table at the worker.
        //     in a context of the given transaction. This scenario is more efficient during
        //     ingests since only a few chunks may get populated during a transaction.
        //   - Otherwise, the scanner relies upon the replica info records. This is a typical
        //     scenario for building the index after publishing a catalog.

        if (hasTransactions()) {
            // The unique combinations of the pairs (chunk,worker) represent replicas.
            // This intermediate data structure is needed to reduce individual chunk contributions
            // into replicas, in order to ensure the results of this version of the chunk screening
            // algorithm will be compatible with expectations of the planner.
            set<pair<unsigned int, string>> chunkAndWorker;

            // Locate all contributions into the table made at the given worker.
            vector<TransactionContribInfo> const contribs =
                    databaseServices->transactionContribs(transactionId(), directorTable(), worker);
            for (auto&& contrib : contribs) {
                chunkAndWorker.insert(make_pair(contrib.chunk, contrib.worker));
            }

            // Transform findings into the input data structure used by the planner.
            for (auto const& elem : chunkAndWorker) {
                chunk2workers[elem.first].push_back(elem.second);
            }
        } else {
            vector<ReplicaInfo> replicas;
            databaseServices->findWorkerReplicas(replicas, worker, database(), unusedAllDatabases,
                                                 unusedIsPublished, includeFileInfo);
            for (auto&& replica : replicas) {
                chunk2workers[replica.chunk()].push_back(replica.worker());
            }
        }
    }

    // ---------------------
    // Stage II: the planner
    // ---------------------

    // Now build the plan for each worker based on the above harvested
    // distribution of chunk replicas across workers.
    //
    // TODO: this single-pass algorithm may be biased to an order
    // in which chunks are being processed by the algorithm. Consider
    // a more sophisticated implementation which would be bias-free.

    for (auto&& itr : chunk2workers) {
        unsigned int const chunk = itr.first;
        auto&& workers = itr.second;

        // Find the least loaded worker from those where chunk replicas
        // are residing.
        string worker;
        size_t minNumChunks = numeric_limits<size_t>::max();
        for (auto&& candidateWorker : workers) {
            auto const numChunksAtCandidate = _chunks[candidateWorker].size();
            if (numChunksAtCandidate < minNumChunks) {
                worker = candidateWorker;
                minNumChunks = numChunksAtCandidate;
            }
        }
        if (worker.empty()) {
            throw logic_error(context() + string(__func__) + ":  internal bug");
        }
        _chunks[worker].push(chunk);
        _totalChunks++;
    }

    // --------------------------------------------------
    // Stage III: launching the initial batch of requests
    // --------------------------------------------------

    // Launch the initial batch of requests in the number which won't exceed
    // the number of the service processing threads at each worker multiplied
    // by the number of workers involved into the operation and by the "magic"
    // number 8. The later is needed to absorb the latency of the network
    // communications so that the worker threads would be able to work on
    // another batch of the data extraction requests while results of the
    // previous batch were being sent back to the Controller.

    size_t const maxRequestsPerWorker = 8 * controller()->serviceProvider()->config()->get<size_t>(
                                                    "worker", "num-svc-processing-threads");

    for (auto&& worker : workerNames) {
        for (auto&& ptr : _launchRequests(lock, worker, maxRequestsPerWorker)) {
            _requests[ptr->id()] = ptr;
        }
    }

    // In case if no workers or database are present in the Configuration
    // at this time.

    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}

void IndexJob::cancelImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // The algorithm will also clear resources taken by various
    // locally created objects.

    _chunks.clear();

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    for (auto&& itr : _requests) {
        auto&& ptr = itr.second;
        ptr->cancel();
        if (ptr->state() != Request::State::FINISHED) {
            controller()->stopById<StopIndexRequest>(ptr->worker(), ptr->id(), nullptr, /* onFinish */
                                                     priority(), true,                  /* keepTracking */
                                                     id()                               /* jobId */
            );
        }
    }
    _requests.clear();
    _rollbackTransaction(__func__);
}

void IndexJob::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<IndexJob>(lock, _onFinish);
}

void IndexJob::_onRequestFinish(IndexRequest::Ptr const& request) {
    // NOTE: this algorithm assumes "zero tolerance" to failures - any failure
    // in executing requests or processing data of the requests would result in
    // the job termination. The only exception from this rule is a scenario
    // when a target chunk table won't have a partition. This may be expected
    // for some chunk tables because they may not have contributions in a context
    // of the given super-transaction.
    //
    // TODO: reconsider this algorithm.

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  worker=" << request->worker());

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    _completeChunks++;

    bool hasData = true;
    if (request->extendedState() != Request::SUCCESS) {
        if (request->extendedServerStatus() == ProtocolStatusExt::NO_SUCH_PARTITION) {
            // OK to proceed. We just don't have any contribution into the
            // partition.
            hasData = false;
        } else {
            _resultData.error[request->worker()][request->chunk()] = request->responseData().error;
            _rollbackTransaction(__func__);
            finish(lock, ExtendedState::FAILED);
            return;
        }
    }

    // Submit a replacement request for the same worker BEFORE processing
    // results of the current one. This little optimization is meant to keep
    // workers busy in case of a non-negligible latency in processing data of
    // requests.

    for (auto&& ptr : _launchRequests(lock, request->worker())) {
        _requests[ptr->id()] = ptr;
    }

    // Removing request from the list before processing its data is fine as
    // we still have a shared pointer passed into this method. Note that
    // we need to erase completed requests from memory since they may carry
    // a significant amount of data.

    _requests.erase(request->id());
    if (hasData) {
        try {
            _processRequestData(lock, request);
        } catch (exception const& ex) {
            string const error = "request data processing failed, ex: " + string(ex.what());
            LOGS(_log, LOG_LVL_ERROR, context() << __func__ << "  " << error);
            _resultData.error[request->worker()][request->chunk()] = error;
            _rollbackTransaction(__func__);
            finish(lock, ExtendedState::FAILED);
            return;
        }
    }

    // Evaluate for the completion condition of the job.
    if (_requests.size() == 0) finish(lock, ExtendedState::SUCCESS);
}

void IndexJob::_processRequestData(util::Lock const& lock, IndexRequest::Ptr const& request) {
    auto const writeIntoFile = [](string const& fileName, ios::openmode mode, string const& data) {
        ofstream f(fileName, mode);
        if (!f.good()) {
            throw runtime_error(typeName() + "::_processRequestData" +
                                "  failed to open/create/append file: " + fileName);
        }
        f << data;
        f.close();
    };
    switch (_destination) {
        case DISCARD:
            break;

        case FILE: {
            if (_destinationPath.empty()) {
                cout << request->responseData().data;
            } else {
                writeIntoFile(_destinationPath, ios::app, request->responseData().data);
            }
            break;
        }
        case FOLDER: {
            string const filePath = (_destinationPath.empty() ? "." : _destinationPath) + "/" + database() +
                                    "_" + to_string(request->chunk()) + ".tsv";
            writeIntoFile(filePath, ios::out | ios::trunc, request->responseData().data);
            break;
        }
        case TABLE: {
            auto const config = controller()->serviceProvider()->config();

            // ATTENTION: all exceptions which may be potentially thrown are
            // supposed to be intercepted by a caller of the current method
            // and be used for error reporting.

            // Dump the data into a temporary file from where it would be loaded
            // into the MySQL table. Note that the file must be readable by
            // the MySQL service.
            //
            // TODO: consider using the named pipe (FIFO)

            string const filePath = config->get<string>("database", "qserv-master-tmp-dir") + "/" +
                                    database() + "_" + to_string(request->chunk()) +
                                    (_hasTransactions ? "_p" + to_string(_transactionId) : "");
            writeIntoFile(filePath, ios::out | ios::trunc, request->responseData().data);

            // Open the database connection if this is the first batch of data
            if (nullptr == _conn) {
                _conn = Connection::open(Configuration::qservCzarDbParams(lsst::qserv::SEC_INDEX_DB));
            }
            QueryGenerator const g(_conn);
            string const query =
                    g.loadDataInfile(filePath, _destinationPath,
                                     config->get<string>("worker", "ingest-charset-name"), _localFile);

            _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
                conn->execute(query);
                // Loading operations based on this mechanism won't result in throwing exceptions in
                // case of certain types of problems encountered during the loading, such as
                // out-of-range data, duplicate keys, etc. These errors are reported as warnings
                // which need to be retrieved using a special call to the database API.
                if (_localFile) {
                    auto const warnings = conn->warnings();
                    if (!warnings.empty()) {
                        auto const& w = warnings.front();
                        throw database::mysql::Error(
                                "query: " + query +
                                " failed with total number of problems: " + to_string(warnings.size()) +
                                ", first problem (Level,Code,Message) was: " + w.level + "," +
                                to_string(w.code) + "," + w.message);
                    }
                }
            });

            // Make the best attempt to get rid of the temporary file. Ignore any errors
            // for now. Just report them.
            boost::system::error_code ec;
            fs::remove(fs::path(filePath), ec);
            if (ec.value() != 0) {
                LOGS(_log, LOG_LVL_ERROR,
                     context() << "::" << __func__ << "  "
                               << "failed to remove the temporary file '" << filePath);
            }
            break;
        }
        default:
            throw range_error(typeName() + "::" + string(__func__) +
                              "  unsupported destination: " + toString(_destination));
    }
}

list<IndexRequest::Ptr> IndexJob::_launchRequests(util::Lock const& lock, string const& worker,
                                                  size_t maxRequests) {
    list<IndexRequest::Ptr> requests;

    // Create as many requests as specified by the corresponding parameter of
    // the method or as many as are still available for the specified
    // worker (not to exceed the limit) by popping chunk numbers from the worker's
    // queue.

    auto const self = shared_from_base<IndexJob>();

    while (_chunks[worker].size() > 0 and requests.size() < maxRequests) {
        auto const chunk = _chunks[worker].front();
        _chunks[worker].pop();

        requests.push_back(controller()->index(
                worker, database(), directorTable(), chunk, hasTransactions(), transactionId(),
                [self](IndexRequest::Ptr const& request) { self->_onRequestFinish(request); }, priority(),
                true, /* keepTracking*/
                id()  /* jobId */
                ));
    }
    return requests;
}

void IndexJob::_rollbackTransaction(string const& func) {
    try {
        if ((nullptr != _conn) and _conn->inTransaction()) _conn->rollback();
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context() << func << "  transaction rollback failed, ex: " << ex.what());
    }
}

}  // namespace lsst::qserv::replica
