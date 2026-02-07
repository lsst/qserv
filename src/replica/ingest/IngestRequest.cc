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
#include "replica/ingest/IngestRequest.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <system_error>
#include <thread>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Client.h"
#include "http/Exceptions.h"
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
string const context_ = "INGEST-REQUEST  ";

/**
 * Class TemporaryCertFileRAII is used for storing certificate bundles in
 * temporary files managed based on the RAII paradigm.
 */
class TemporaryCertFileRAII {
public:
    /// The default constructor won't create any file.
    TemporaryCertFileRAII() = default;

    TemporaryCertFileRAII(TemporaryCertFileRAII const&) = delete;
    TemporaryCertFileRAII& operator=(TemporaryCertFileRAII const&) = delete;

    /// The destructor will take care of deleting a file should the one be created.
    ~TemporaryCertFileRAII() {
        // Make the best effort to delete the file. Ignore any errors.
        if (!_fileName.empty()) {
            std::error_code ec;
            fs::remove(fs::path(_fileName), ec);
        }
    }

    /**
     * Create a temporary file and write a certificate bundle into it.
     * @param baseDir A folder where the file is created.
     * @param database The name of a database for which the file gets created.
     * @param cert The certificate bundle to be written into the file.
     * @return A path to the file including its folder.
     * @throw http::Error If the file couldn't be open for writing.
     */
    string write(string const& baseDir, string const& database, string const& cert) {
        string const prefix = database + "-";
        string const model = "%%%%-%%%%-%%%%-%%%%";
        string const suffix = ".cert";
        unsigned int const maxRetries = 1;
        _fileName = FileUtils::createTemporaryFile(baseDir, prefix, model, suffix, maxRetries);
        ofstream fs;
        fs.open(_fileName, ios::out | ios::trunc);
        if (!fs.is_open()) {
            lsst::qserv::http::raiseRetryAllowedError("TemporaryCertFileRAII::" + string(__func__),
                                                      "failed to open/create file '" + _fileName + "'.");
        }
        fs << cert;
        fs.close();
        return _fileName;
    }

private:
    string _fileName;
};
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<IngestRequest> IngestRequest::create(shared_ptr<ServiceProvider> const& serviceProvider,
                                                string const& workerName, TransactionId transactionId,
                                                string const& table, unsigned int chunk, bool isOverlap,
                                                string const& url, string const& charsetName, bool async,
                                                csv::DialectInput const& dialectInput,
                                                http::Method httpMethod, string const& httpData,
                                                vector<string> const& httpHeaders,
                                                unsigned int maxNumWarnings, unsigned int maxRetries) {
    shared_ptr<IngestRequest> ptr(new IngestRequest(
            serviceProvider, workerName, transactionId, table, chunk, isOverlap, url, charsetName, async,
            dialectInput, httpMethod, httpData, httpHeaders, maxNumWarnings, maxRetries));
    return ptr;
}

shared_ptr<IngestRequest> IngestRequest::resume(shared_ptr<ServiceProvider> const& serviceProvider,
                                                string const& workerName, unsigned int contribId) {
    string const context = ::context_ + string(__func__) + " ";
    auto const config = serviceProvider->config();
    auto const databaseServices = serviceProvider->databaseServices();

    // Find the request in the database and run some preliminary validation of its
    // state to ensure the request is eligible to be resumed.
    TransactionContribInfo contrib;
    try {
        contrib = databaseServices->transactionContrib(contribId);
    } catch (exception const& ex) {
        throw runtime_error(context + "failed to locate the contribution id=" + to_string(contribId) +
                            " in the database.");
    }
    if (contrib.status != TransactionContribInfo::Status::IN_PROGRESS) {
        throw invalid_argument(
                "contribution id=" + to_string(contribId) + " is not in state " +
                TransactionContribInfo::status2str(TransactionContribInfo::Status::IN_PROGRESS) +
                ", the actual state is " + TransactionContribInfo::status2str(contrib.status) + ".");
    }
    if (!contrib.async) {
        throw invalid_argument("contribution id=" + to_string(contribId) + " is not ASYNC.");
    }

    // Note that contrib.startTime doesn't need to be validated since it's allowed
    // to resume requests that have not been started yet or which are still in an early
    // processing state (before the final stage when changes to MySQL are about to be
    // made or have been made).
    if ((contrib.createTime == 0) || (contrib.readTime != 0) || (contrib.loadTime != 0)) {
        throw invalid_argument("contribution id=" + to_string(contribId) +
                               " is not eligible to be resumed since"
                               " changes to the MySQL table may have already been made.");
    }

    auto const trans = databaseServices->transaction(contrib.transactionId);
    auto const database = config->databaseInfo(trans.database);
    try {
        IngestRequest::_validateState(trans, database, contrib);
    } catch (exception const& ex) {
        contrib.status = TransactionContribInfo::Status::CREATE_FAILED;
        contrib.error = context + ex.what();
        contrib.retryAllowed = false;
        contrib = databaseServices->updateTransactionContrib(contrib);
        throw invalid_argument(contrib.error);
    }

    // Make sure the state is clear (except the contrib.id and contrib.createTime
    // which need to be retained)
    contrib.startTime = 0;
    contrib.tmpFile.clear();
    contrib.error.clear();
    contrib.httpError = 0;
    contrib.systemError = 0;
    contrib.retryAllowed = false;
    contrib = databaseServices->updateTransactionContrib(contrib);

    return shared_ptr<IngestRequest>(new IngestRequest(serviceProvider, workerName, contrib));
}

shared_ptr<IngestRequest> IngestRequest::test(TransactionContribInfo const& contrib) {
    return shared_ptr<IngestRequest>(new IngestRequest(contrib));
}

shared_ptr<IngestRequest> IngestRequest::createRetry(shared_ptr<ServiceProvider> const& serviceProvider,
                                                     string const& workerName, unsigned int contribId,
                                                     bool async) {
    string const context = ::context_ + string(__func__) + " ";
    auto const config = serviceProvider->config();
    auto const databaseServices = serviceProvider->databaseServices();

    // Find the request in the database and run some preliminary validation of its
    // state to ensure the request is eligible to be resumed.
    TransactionContribInfo contrib;
    try {
        contrib = databaseServices->transactionContrib(contribId);
    } catch (exception const& ex) {
        throw runtime_error(context + "failed to locate the contribution id=" + to_string(contribId) +
                            " in the database.");
    }
    if (contrib.status != TransactionContribInfo::Status::READ_FAILED) {
        throw invalid_argument(
                "contribution id=" + to_string(contribId) + " is not in state " +
                TransactionContribInfo::status2str(TransactionContribInfo::Status::READ_FAILED) +
                ", the actual state is " + TransactionContribInfo::status2str(contrib.status) + ".");
    }
    if (contrib.worker != workerName) {
        throw invalid_argument("contribution id=" + to_string(contribId) +
                               " was originally processed by worker '" + contrib.worker +
                               "', while this retry operation was request at worker '" + workerName + "'.");
    }

    // Move counters and error status codes from the contribution object
    // into the retry. The corresponding fields of the contribution objects
    // will get reset to the initial values (which are the same as in the default
    // constructed retry object). Then update the persistent state.
    TransactionContribInfo::FailedRetry const failedRetry =
            contrib.resetForRetry(TransactionContribInfo::Status::IN_PROGRESS, async);
    contrib = databaseServices->updateTransactionContrib(contrib);

    // The retry object has to be saved in the persistent state separately.
    contrib.failedRetries.push_back(failedRetry);
    contrib.numFailedRetries = contrib.failedRetries.size();
    contrib = databaseServices->saveLastTransactionContribRetry(contrib);

    return shared_ptr<IngestRequest>(new IngestRequest(serviceProvider, workerName, contrib));
}

void IngestRequest::_validateState(TransactionInfo const& trans, DatabaseInfo const& database,
                                   TransactionContribInfo const& contrib) {
    string error;
    if (database.isPublished) {
        error = "database '" + database.name + "' is already published.";
    } else if (database.findTable(contrib.table).isPublished) {
        error = "table '" + contrib.table + "' of database '" + database.name + "' is already published.";
    } else if (trans.state != TransactionInfo::State::STARTED) {
        error = "transactionId=" + to_string(contrib.transactionId) + " is not active";
    }
    if (!error.empty()) throw logic_error(error);
}

IngestRequest::IngestRequest(shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
                             TransactionId transactionId, string const& table, unsigned int chunk,
                             bool isOverlap, string const& url, string const& charsetName, bool async,
                             csv::DialectInput const& dialectInput, http::Method httpMethod,
                             string const& httpData, vector<string> const& httpHeaders,
                             unsigned int maxNumWarnings, unsigned int maxRetries)
        : IngestFileSvc(serviceProvider, workerName) {
    // Initialize the descriptor
    TransactionContribInfo contrib;
    contrib.transactionId = transactionId;
    contrib.table = table;
    contrib.chunk = chunk;
    contrib.isOverlap = isOverlap;
    contrib.worker = workerName;
    contrib.url = url;
    contrib.charsetName = charsetName;
    contrib.async = async;
    contrib.dialectInput = dialectInput;
    contrib.httpMethod = httpMethod;
    contrib.httpData = httpData;
    contrib.httpHeaders = httpHeaders;
    if (maxNumWarnings == 0) {
        contrib.maxNumWarnings =
                serviceProvider->config()->get<unsigned int>("worker", "loader-max-warnings");
    } else {
        contrib.maxNumWarnings = maxNumWarnings;
    }
    contrib.maxRetries = std::min(
            maxRetries, serviceProvider->config()->get<unsigned int>("worker", "ingest-max-retries"));

    // Prescreen parameters of the request to ensure the request has a valid
    // context (transaction, database, table). Refuse to proceed with registering
    // the contribution should any issues were detected when locating the context.
    string const context = ::context_ + string(__func__) + " ";
    bool const failed = true;
    auto const config = serviceProvider->config();
    auto const databaseServices = serviceProvider->databaseServices();
    auto const trans = databaseServices->transaction(contrib.transactionId);

    contrib.database = trans.database;

    DatabaseInfo const database = config->databaseInfo(contrib.database);
    if (!database.tableExists(contrib.table)) {
        throw invalid_argument(context + "no such table '" + contrib.table + "' in database '" +
                               contrib.database + "'.");
    }

    // Any failures detected hereafter will result in registering the contribution
    // as failed for further analysis by the ingest workflows.
    try {
        IngestRequest::_validateState(trans, database, contrib);
        _resource.reset(new http::Url(contrib.url));
        switch (_resource->scheme()) {
            case http::Url::FILE:
            case http::Url::HTTP:
            case http::Url::HTTPS:
                break;
            default:
                throw invalid_argument(context + "unsupported url '" + contrib.url + "'");
        }
        _dialect = csv::Dialect(dialectInput);
    } catch (exception const& ex) {
        auto contrib = transactionContribInfo();
        contrib.error = context + ex.what();
        contrib.retryAllowed = false;
        _updateTransactionContribInfo(databaseServices->createdTransactionContrib(contrib, failed));
        throw;
    }
    _updateTransactionContribInfo(databaseServices->createdTransactionContrib(contrib));
}

IngestRequest::IngestRequest(shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
                             TransactionContribInfo const& contrib)
        : IngestFileSvc(serviceProvider, workerName), _contrib(contrib) {
    // This constructor assumes a valid contribution object obtained from a database
    // was passed into the method.
    _resource.reset(new http::Url(_contrib.url));
    _dialect = csv::Dialect(_contrib.dialectInput);
}

IngestRequest::IngestRequest(TransactionContribInfo const& contrib)
        : IngestFileSvc(shared_ptr<ServiceProvider>(), string()), _contrib(contrib) {}

TransactionContribInfo IngestRequest::transactionContribInfo() const {
    string const context = ::context_ + string(__func__) + " ";
    replica::Lock lock(_contribMtx, context);
    return _contrib;
}

void IngestRequest::_updateTransactionContribInfo(TransactionContribInfo const& contrib) {
    string const context = ::context_ + string(__func__) + " ";
    replica::Lock lock(_contribMtx, context);
    _contrib = contrib;
}

void IngestRequest::process() {
    // No actual processing for the test requests made for unit testing.
    if (serviceProvider() == nullptr) return;

    // Request processing is split into 3 stages to allow interrupting the processing
    // if the request has been canceled.
    _processStart();
    _processReadData();
    _processLoadData();
}

void IngestRequest::cancel() {
    // No actual cancellation for the test requests made for unit testing.
    if (serviceProvider() == nullptr) return;

    // A result from setting the flag will depend on a state of the request.
    // If the requests is already being processed it's up to the processing thread
    // to take actions on the delayed cancellation (if it's not to late for the request).
    _cancelled = true;
}

void IngestRequest::_processStart() {
    string const context = ::context_ + string(__func__) + " ";
    replica::Lock const lock(_mtx, context);

    if (_processing.exchange(true)) {
        throw logic_error(context + "the contribution request " + to_string(_contrib.id) +
                          " is already being processed or has been processed.");
    }

    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();
    if (_cancelled) {
        auto contrib = transactionContribInfo();
        contrib.error = "cancelled before beginning processing the request.";
        contrib.retryAllowed = true;
        _updateTransactionContribInfo(databaseServices->startedTransactionContrib(
                contrib, failed, TransactionContribInfo::Status::CANCELLED));
        throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
    }

    // Validate the request to see if it's still valid in the current context.
    // Exceptions will be thrown if the context of the contribution
    // has disappeared while the contribution was sitting in
    // the input queue. Note that updating the status of the contribution
    // in the Replication database won't be possible should this kind
    // of a change happened.
    auto const trans = databaseServices->transaction(_contrib.transactionId);
    auto const database = serviceProvider()->config()->databaseInfo(trans.database);
    if (!database.tableExists(_contrib.table)) {
        throw invalid_argument(context + "no such table '" + _contrib.table + "' exists in database '" +
                               _contrib.database + "'.");
    }
    // Verify if any change in the status of the targeted context has happened
    // since a time the contribution request was made. Note that retrying
    // the same contribution would be prohibited should this happened.
    try {
        IngestRequest::_validateState(trans, database, _contrib);
    } catch (exception const& ex) {
        auto contrib = transactionContribInfo();
        contrib.error = context + ex.what();
        contrib.retryAllowed = false;
        _updateTransactionContribInfo(databaseServices->startedTransactionContrib(contrib, failed));
        throw;
    }

    // The actual processing of the request begins with opening a temporary file
    // where the preprocessed content of the contribution will be stored.
    _openTmpFileAndStart(lock);
}

void IngestRequest::_openTmpFileAndStart(replica::Lock const& lock) {
    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();
    try {
        auto contrib = transactionContribInfo();
        contrib.tmpFile = openFile(contrib.transactionId, contrib.table, _dialect, contrib.charsetName,
                                   contrib.chunk, contrib.isOverlap);
        _updateTransactionContribInfo(databaseServices->startedTransactionContrib(contrib));
    } catch (http::Error const& ex) {
        auto contrib = transactionContribInfo();
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            contrib.httpError = errorExt["http_error"];
            contrib.systemError = errorExt["system_error"];
        }
        contrib.error = ex.what();
        contrib.retryAllowed = true;
        _updateTransactionContribInfo(databaseServices->startedTransactionContrib(contrib, failed));
        throw;
    } catch (exception const& ex) {
        auto contrib = transactionContribInfo();
        contrib.systemError = errno;
        contrib.error = ex.what();
        contrib.retryAllowed = true;
        _updateTransactionContribInfo(databaseServices->startedTransactionContrib(contrib, failed));
        throw;
    }
}

void IngestRequest::_processReadData() {
    string const context = ::context_ + string(__func__) + " ";
    replica::Lock const lock(_mtx, context);

    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();

    // Loop over retries (if any). The loop terminates if the file was successfuly read/processed
    // or after hitting the limit of retries set for the request.
    while (true) {
        // Start reading and preprocessing the input file.
        if (_cancelled) {
            auto contrib = transactionContribInfo();
            contrib.error = "cancelled before reading the input file.";
            contrib.retryAllowed = true;
            _updateTransactionContribInfo(databaseServices->readTransactionContrib(
                    contrib, failed, TransactionContribInfo::Status::CANCELLED));
            closeFile();
            throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
        }
        try {
            switch (_resource->scheme()) {
                case http::Url::FILE:
                    _readLocalFile(lock);
                    break;
                case http::Url::HTTP:
                case http::Url::HTTPS:
                    _readRemoteFile(lock);
                    break;
                default:
                    throw invalid_argument(context + "unsupported url '" + _contrib.url + "'");
            }
            _updateTransactionContribInfo(databaseServices->readTransactionContrib(_contrib));
            return;
        } catch (http::Error const& ex) {
            auto contrib = transactionContribInfo();
            json const errorExt = ex.errorExt();
            if (!errorExt.empty()) {
                contrib.httpError = errorExt["http_error"];
                contrib.systemError = errorExt["system_error"];
            }
            contrib.error = ex.what();
            contrib.retryAllowed = true;
            _updateTransactionContribInfo(databaseServices->readTransactionContrib(contrib, failed));
            if (!_closeTmpFileAndRetry(lock)) throw;
        } catch (exception const& ex) {
            auto contrib = transactionContribInfo();
            contrib.systemError = errno;
            contrib.error = ex.what();
            contrib.retryAllowed = true;
            _updateTransactionContribInfo(databaseServices->readTransactionContrib(contrib, failed));
            if (!_closeTmpFileAndRetry(lock)) throw;
        }
    }
}

bool IngestRequest::_closeTmpFileAndRetry(replica::Lock const& lock) {
    closeFile();
    if (_contrib.numFailedRetries >= _contrib.maxRetries) return false;

    // Prepare a context for the next attempt to read the contribution.

    // Move counters and error status codes from the contribution object
    // into the retry. The corresponding fields of the contribution objects
    // will get reset to the initial values (which are the same as in the default
    // constructed retry object).
    auto contrib = transactionContribInfo();
    TransactionContribInfo::FailedRetry const failedRetry =
            contrib.resetForRetry(contrib.status, contrib.async);
    _updateTransactionContribInfo(contrib);

    // This method will open the new temporary file save the updated state of
    // the contribution to prepare the current context for the next attempt
    // to read the input data.
    _openTmpFileAndStart(lock);

    // The retry object has to be saved separately.
    contrib = transactionContribInfo();
    contrib.failedRetries.push_back(failedRetry);
    contrib.numFailedRetries = contrib.failedRetries.size();
    _updateTransactionContribInfo(
            serviceProvider()->databaseServices()->saveLastTransactionContribRetry(contrib));

    return true;
}

void IngestRequest::_processLoadData() {
    string const context = ::context_ + string(__func__) + " ";
    replica::Lock const lock(_mtx, context);

    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();

    // Load the preprocessed input file into MySQL and update the persistent
    // state of the contribution request.
    if (_cancelled) {
        auto contrib = transactionContribInfo();
        contrib.error = "cancelled before loading data into MySQL";
        contrib.retryAllowed = true;
        _updateTransactionContribInfo(databaseServices->loadedTransactionContrib(
                contrib, failed, TransactionContribInfo::Status::CANCELLED));
        closeFile();
        throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
    }
    try {
        loadDataIntoTable(_contrib.maxNumWarnings);
        auto contrib = transactionContribInfo();
        contrib.numWarnings = numWarnings();
        contrib.warnings = warnings();
        contrib.numRowsLoaded = numRowsLoaded();
        _updateTransactionContribInfo(databaseServices->loadedTransactionContrib(contrib));
    } catch (exception const& ex) {
        auto contrib = transactionContribInfo();
        contrib.systemError = errno;
        contrib.error = ex.what();
        _updateTransactionContribInfo(databaseServices->loadedTransactionContrib(contrib, failed));
        closeFile();
        throw;
    }
    closeFile();
}

void IngestRequest::_readLocalFile(replica::Lock const& lock) {
    string const context = ::context_ + string(__func__) + " ";

    uint64_t numBytes = 0;
    uint64_t numRows = 0;

    unique_ptr<char[]> const record(new char[defaultRecordSizeBytes]);
    ifstream infile(_resource->filePath(), ios::binary);
    if (!infile.is_open()) {
        http::raiseRetryAllowedError(context, "failed to open the file '" + _resource->filePath() +
                                                      "', error: '" + strerror(errno) +
                                                      "', errno: " + to_string(errno));
    }
    auto parser = make_unique<csv::Parser>(_dialect);
    bool eof = false;
    do {
        eof = !infile.read(record.get(), defaultRecordSizeBytes);
        if (eof && !infile.eof()) {
            http::raiseRetryAllowedError(context, "failed to read the file '" + _resource->filePath() +
                                                          "', error: '" + strerror(errno) +
                                                          "', errno: " + to_string(errno));
        }
        size_t const num = infile.gcount();
        numBytes += num;
        // Flush the last record if the end of the file.
        parser->parse(record.get(), num, eof, [&](char const* buf, size_t size) {
            writeRowIntoFile(buf, size);
            numRows++;
        });
    } while (!eof);

    auto contrib = transactionContribInfo();
    contrib.numBytes = numBytes;
    contrib.numRows = numRows;
    _updateTransactionContribInfo(contrib);
}

void IngestRequest::_readRemoteFile(replica::Lock const& lock) {
    uint64_t numBytes = 0;
    uint64_t numRows = 0;

    auto const reportRow = [&](char const* buf, size_t size) {
        writeRowIntoFile(buf, size);
        numRows++;
    };

    // The configuration may be updated later if certificate bundles were loaded
    // by a client into the config store.
    auto clientConfig = _clientConfig(lock);

    // Check if values of the certificate bundles were loaded into the configuration
    // store for the catalog. If so then write the certificates into temporary files
    // at the work folder configured to support HTTP-based file ingest operations.
    // The files are managed by the RAII resources, and they will get automatically
    // removed after successfully finishing reading the remote file or in case of any
    // exceptions.

    ::TemporaryCertFileRAII caInfoFile;
    if (!clientConfig.caInfoVal.empty()) {
        // Use this file instead of the existing path.
        clientConfig.caInfo =
                caInfoFile.write(serviceProvider()->config()->get<string>("worker", "http-loader-tmp-dir"),
                                 _contrib.database, clientConfig.caInfoVal);
    }
    ::TemporaryCertFileRAII proxyCaInfoFile;
    if (!clientConfig.proxyCaInfoVal.empty()) {
        // Use this file instead of the existing path.
        clientConfig.proxyCaInfo = proxyCaInfoFile.write(
                serviceProvider()->config()->get<string>("worker", "http-loader-tmp-dir"), _contrib.database,
                clientConfig.proxyCaInfoVal);
    }

    // Read and parse data from the data source
    auto parser = make_unique<csv::Parser>(_dialect);
    bool const flush = true;
    http::Client reader(_contrib.httpMethod, _contrib.url, _contrib.httpData, _contrib.httpHeaders,
                        clientConfig);
    reader.read([&](char const* record, size_t size) {
        parser->parse(record, size, !flush, reportRow);
        numBytes += size;
    });
    // Flush the last non-terminated line stored in the parser (if any).
    string const emptyRecord;
    parser->parse(emptyRecord.data(), emptyRecord.size(), flush, reportRow);

    auto contrib = transactionContribInfo();
    contrib.numBytes = numBytes;
    contrib.numRows = numRows;
    _updateTransactionContribInfo(contrib);
}

http::ClientConfig IngestRequest::_clientConfig(replica::Lock const& lock) const {
    auto const databaseServices = serviceProvider()->databaseServices();
    auto const getString = [&](string& val, string const& key) -> bool {
        try {
            val = databaseServices->ingestParam(_contrib.database, http::ClientConfig::category, key).value;
        } catch (DatabaseServicesNotFound const&) {
            return false;
        }
        return true;
    };
    auto const getBool = [&getString](bool& val, string const& key) {
        string str;
        if (getString(str, key)) val = stoi(str) != 0;
    };
    auto const getLong = [&getString](long& val, string const& key) {
        string str;
        if (getString(str, key)) val = stol(str);
    };
    http::ClientConfig clientConfig;
    getBool(clientConfig.sslVerifyHost, http::ClientConfig::sslVerifyHostKey);
    getBool(clientConfig.sslVerifyPeer, http::ClientConfig::sslVerifyPeerKey);
    getString(clientConfig.caPath, http::ClientConfig::caPathKey);
    getString(clientConfig.caInfo, http::ClientConfig::caInfoKey);
    getString(clientConfig.caInfoVal, http::ClientConfig::caInfoValKey);
    getBool(clientConfig.proxySslVerifyHost, http::ClientConfig::proxySslVerifyHostKey);
    getBool(clientConfig.proxySslVerifyPeer, http::ClientConfig::proxySslVerifyPeerKey);
    getString(clientConfig.proxyCaPath, http::ClientConfig::proxyCaPathKey);
    getString(clientConfig.proxyCaInfo, http::ClientConfig::proxyCaInfoKey);
    getString(clientConfig.proxyCaInfoVal, http::ClientConfig::proxyCaInfoValKey);
    getString(clientConfig.proxy, http::ClientConfig::proxyKey);
    getString(clientConfig.noProxy, http::ClientConfig::noProxyKey);
    getLong(clientConfig.httpProxyTunnel, http::ClientConfig::httpProxyTunnelKey);
    getLong(clientConfig.connectTimeout, http::ClientConfig::connectTimeoutKey);
    getLong(clientConfig.timeout, http::ClientConfig::timeoutKey);
    getLong(clientConfig.lowSpeedLimit, http::ClientConfig::lowSpeedLimitKey);
    getLong(clientConfig.lowSpeedTime, http::ClientConfig::lowSpeedTimeKey);
    return clientConfig;
}

}  // namespace lsst::qserv::replica
