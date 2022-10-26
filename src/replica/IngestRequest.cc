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
#include "replica/IngestRequest.h"

// System headers
#include <cerrno>
#include <cstring>
#include <fstream>
#include <thread>

// Third party headers
#include "boost/filesystem.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/HttpClient.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
namespace fs = boost::filesystem;
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
            boost::system::error_code ec;
            fs::remove(fs::path(_fileName), ec);
        }
    }

    /**
     * Create a temporary file and write a certificate bundle into it.
     * @param baseDir A folder where the file is created.
     * @param database The name of a database for which the file gets created.
     * @param cert The certificate bundle to be written into the file.
     * @return A path to the file including its folder.
     * @throw HttpError If the file couldn't be open for writing.
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
            raiseRetryAllowedError("TemporaryCertFileRAII::" + string(__func__),
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

shared_ptr<IngestRequest> IngestRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
        TransactionId transactionId, string const& table, unsigned int chunk, bool isOverlap,
        string const& url, bool async, csv::DialectInput const& dialectInput, string const& httpMethod,
        string const& httpData, vector<string> const& httpHeaders, unsigned int maxNumWarnings) {
    shared_ptr<IngestRequest> ptr(new IngestRequest(serviceProvider, workerName, transactionId, table, chunk,
                                                    isOverlap, url, async, dialectInput, httpMethod, httpData,
                                                    httpHeaders, maxNumWarnings));
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
                             bool isOverlap, string const& url, bool async,
                             csv::DialectInput const& dialectInput, string const& httpMethod,
                             string const& httpData, vector<string> const& httpHeaders,
                             unsigned int maxNumWarnings)
        : IngestFileSvc(serviceProvider, workerName) {
    // Initialize the descriptor
    _contrib.transactionId = transactionId;
    _contrib.table = table;
    _contrib.chunk = chunk;
    _contrib.isOverlap = isOverlap;
    _contrib.worker = workerName;
    _contrib.url = url;
    _contrib.async = async;
    _contrib.dialectInput = dialectInput;
    _contrib.httpMethod = httpMethod;
    _contrib.httpData = httpData;
    _contrib.httpHeaders = httpHeaders;
    if (maxNumWarnings == 0) {
        _contrib.maxNumWarnings =
                serviceProvider->config()->get<unsigned int>("worker", "loader-max-warnings");
    } else {
        _contrib.maxNumWarnings = maxNumWarnings;
    }

    // Prescreen parameters of the request to ensure the request has a valid
    // context (transaction, database, table). Refuse to proceed with registering
    // the contribution should any issues were detected when locating the context.
    string const context = ::context_ + string(__func__) + " ";
    bool const failed = true;
    auto const config = serviceProvider->config();
    auto const databaseServices = serviceProvider->databaseServices();
    auto const trans = databaseServices->transaction(_contrib.transactionId);

    _contrib.database = trans.database;

    DatabaseInfo const database = config->databaseInfo(_contrib.database);
    if (!database.tableExists(_contrib.table)) {
        throw invalid_argument(context + "no such table '" + _contrib.table + "' in database '" +
                               _contrib.database + "'.");
    }

    // Any failures detected hereafter will result in registering the contribution
    // as failed for further analysis by the ingest workflows.
    try {
        IngestRequest::_validateState(trans, database, _contrib);
        _resource.reset(new Url(_contrib.url));
        switch (_resource->scheme()) {
            case Url::FILE:
            case Url::HTTP:
            case Url::HTTPS:
                break;
            default:
                throw invalid_argument(context + "unsupported url '" + _contrib.url + "'");
        }
        _dialect = csv::Dialect(dialectInput);
    } catch (exception const& ex) {
        _contrib.error = context + ex.what();
        _contrib.retryAllowed = false;
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        throw;
    }
    _contrib = databaseServices->createdTransactionContrib(_contrib);
}

IngestRequest::IngestRequest(shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName,
                             TransactionContribInfo const& contrib)
        : IngestFileSvc(serviceProvider, workerName), _contrib(contrib) {
    // This constructor assumes a valid contribution object obtained from a database
    // was passed into the method.
    _resource.reset(new Url(_contrib.url));
    _dialect = csv::Dialect(_contrib.dialectInput);
}

IngestRequest::IngestRequest(TransactionContribInfo const& contrib)
        : IngestFileSvc(shared_ptr<ServiceProvider>(), string()), _contrib(contrib) {}

TransactionContribInfo IngestRequest::transactionContribInfo() const {
    string const context = ::context_ + string(__func__) + " ";
    util::Lock lock(_mtx, context);
    return _contrib;
}

void IngestRequest::process() {
    // No actual processing for the test requests made for unit testing.
    if (serviceProvider() == nullptr) return;

    string const context = ::context_ + string(__func__) + " ";
    {
        util::Lock lock(_mtx, context);
        if (_processing) {
            throw logic_error(context + "the contribution request " + to_string(_contrib.id) +
                              " is already being processed or has been processed.");
        }
        if (_cancelled) {
            throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) +
                                           " is already cancelled");
        }
        // Exceptions will be thrown if the context of the contribution
        // has disappeared while the contribution was sitting in
        // the input queue. Note that updating the status of the contribution
        // in the Replication database won't be possible should this kind
        // of a change happened.
        auto const config = serviceProvider()->config();
        auto const databaseServices = serviceProvider()->databaseServices();
        auto const trans = databaseServices->transaction(_contrib.transactionId);
        auto const database = config->databaseInfo(trans.database);
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
            _contrib.error = context + ex.what();
            _contrib.retryAllowed = false;
            bool const failed = true;
            _contrib = databaseServices->startedTransactionContrib(
                    _contrib, failed, TransactionContribInfo::Status::START_FAILED);
            throw;
        }
        _processing = true;
    }
    _processStart();
    _processReadData();
    _processLoadData();
}

void IngestRequest::cancel() {
    // No actual cancellation for the test requests made for unit testing.
    if (serviceProvider() == nullptr) return;

    string const context = ::context_ + string(__func__) + " ";
    util::Lock lock(_mtx, context);

    // A result from setting the flag will depend on a state of the request.
    // If the requests is already being processed it's up to the processing thread
    // to take actions on the delayed cancellation (if it's not to late for the request).
    _cancelled = true;
    if (!_processing) {
        // Cancel the request immediately to prevent any further changes to the state
        // of the request.
        bool const failed = true;
        _contrib = serviceProvider()->databaseServices()->startedTransactionContrib(
                _contrib, failed, TransactionContribInfo::Status::CANCELLED);
    }
}

void IngestRequest::_processStart() {
    string const context = ::context_ + string(__func__) + " ";
    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();

    // The actual processing of the request begins with open a temporary file
    // where the preprocessed content of the contribution will be stored.
    {
        util::Lock lock(_mtx, context);
        if (_cancelled) {
            _contrib.error = "cancelled before opening a temporary file.";
            _contrib.retryAllowed = true;
            _contrib = databaseServices->startedTransactionContrib(_contrib, failed,
                                                                   TransactionContribInfo::Status::CANCELLED);
            throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
        }
    }
    try {
        _contrib.tmpFile = openFile(_contrib.transactionId, _contrib.table, _dialect, _contrib.chunk,
                                    _contrib.isOverlap);
        util::Lock lock(_mtx, context);
        _contrib = databaseServices->startedTransactionContrib(_contrib);

    } catch (HttpError const& ex) {
        util::Lock lock(_mtx, context);
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            _contrib.httpError = errorExt["http_error"];
            _contrib.systemError = errorExt["system_error"];
        }
        _contrib.error = ex.what();
        _contrib.retryAllowed = true;
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        throw;
    } catch (exception const& ex) {
        util::Lock lock(_mtx, context);
        _contrib.systemError = errno;
        _contrib.error = ex.what();
        _contrib.retryAllowed = true;
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        throw;
    }
}

void IngestRequest::_processReadData() {
    string const context = ::context_ + string(__func__) + " ";
    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();

    // Start reading and preprocessing the input file.
    {
        util::Lock lock(_mtx, context);
        if (_cancelled) {
            _contrib.error = "cancelled before reading the input file.";
            _contrib.retryAllowed = true;
            _contrib = databaseServices->readTransactionContrib(_contrib, failed,
                                                                TransactionContribInfo::Status::CANCELLED);
            closeFile();
            throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
        }
    }
    try {
        switch (_resource->scheme()) {
            case Url::FILE:
                _readLocalFile();
                break;
            case Url::HTTP:
            case Url::HTTPS:
                _readRemoteFile();
                break;
            default:
                throw invalid_argument(string(__func__) + " unsupported url '" + _contrib.url + "'");
        }
        util::Lock lock(_mtx, context);
        _contrib = databaseServices->readTransactionContrib(_contrib);
    } catch (HttpError const& ex) {
        util::Lock lock(_mtx, context);
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            _contrib.httpError = errorExt["http_error"];
            _contrib.systemError = errorExt["system_error"];
        }
        _contrib.error = ex.what();
        _contrib.retryAllowed = true;
        _contrib = databaseServices->readTransactionContrib(_contrib, failed);
        closeFile();
        throw;
    } catch (exception const& ex) {
        util::Lock lock(_mtx, context);
        _contrib.systemError = errno;
        _contrib.error = ex.what();
        _contrib.retryAllowed = true;
        _contrib = databaseServices->readTransactionContrib(_contrib, failed);
        closeFile();
        throw;
    }
}

void IngestRequest::_processLoadData() {
    string const context = ::context_ + string(__func__) + " ";
    bool const failed = true;
    auto const databaseServices = serviceProvider()->databaseServices();

    // Load the preprocessed input file into MySQL and update the persistent
    // state of the contribution request.
    {
        util::Lock lock(_mtx, context);
        if (_cancelled) {
            _contrib.error = "cancelled before loading data into MySQL";
            _contrib.retryAllowed = true;
            _contrib = databaseServices->loadedTransactionContrib(_contrib, failed,
                                                                  TransactionContribInfo::Status::CANCELLED);
            closeFile();
            throw IngestRequestInterrupted(context + "request " + to_string(_contrib.id) + _contrib.error);
        }
    }
    try {
        loadDataIntoTable(_contrib.maxNumWarnings);
        util::Lock lock(_mtx, context);
        _contrib.numWarnings = numWarnings();
        _contrib.warnings = warnings();
        _contrib.numRowsLoaded = numRowsLoaded();
        _contrib = databaseServices->loadedTransactionContrib(_contrib);
    } catch (exception const& ex) {
        {
            util::Lock lock(_mtx, context);
            _contrib.systemError = errno;
            _contrib.error = ex.what();
            _contrib = databaseServices->loadedTransactionContrib(_contrib, failed);
        }
        closeFile();
        throw;
    }
    closeFile();
}

void IngestRequest::_readLocalFile() {
    string const context = ::context_ + string(__func__) + " ";

    _contrib.numBytes = 0;
    _contrib.numRows = 0;

    unique_ptr<char[]> const record(new char[defaultRecordSizeBytes]);
    ifstream infile(_resource->filePath(), ios::binary);
    if (!infile.is_open()) {
        raiseRetryAllowedError(context, "failed to open the file '" + _resource->filePath() + "', error: '" +
                                                strerror(errno) + "', errno: " + to_string(errno));
    }
    auto parser = make_unique<csv::Parser>(_dialect);
    bool eof = false;
    do {
        eof = !infile.read(record.get(), defaultRecordSizeBytes);
        if (eof && !infile.eof()) {
            raiseRetryAllowedError(context, "failed to read the file '" + _resource->filePath() +
                                                    "', error: '" + strerror(errno) +
                                                    "', errno: " + to_string(errno));
        }
        size_t const num = infile.gcount();
        _contrib.numBytes += num;
        // Flush the last record if the end of the file.
        parser->parse(record.get(), num, eof, [&](char const* buf, size_t size) {
            writeRowIntoFile(buf, size);
            _contrib.numRows++;
        });
    } while (!eof);
}

void IngestRequest::_readRemoteFile() {
    _contrib.numBytes = 0;
    _contrib.numRows = 0;

    auto const reportRow = [&](char const* buf, size_t size) {
        writeRowIntoFile(buf, size);
        _contrib.numRows++;
    };

    // The configuration may be updated later if certificate bundles were loaded
    // by a client into the config store.
    auto clientConfig = _clientConfig();

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
    HttpClient reader(_contrib.httpMethod, _contrib.url, _contrib.httpData, _contrib.httpHeaders,
                      clientConfig);
    reader.read([&](char const* record, size_t size) {
        parser->parse(record, size, !flush, reportRow);
        _contrib.numBytes += size;
    });
    // Flush the last non-terminated line stored in the parser (if any).
    string const emptyRecord;
    parser->parse(emptyRecord.data(), emptyRecord.size(), flush, reportRow);
}

HttpClientConfig IngestRequest::_clientConfig() const {
    auto const databaseServices = serviceProvider()->databaseServices();
    auto const getString = [&](string& val, string const& key) -> bool {
        try {
            val = databaseServices->ingestParam(_contrib.database, HttpClientConfig::category, key).value;
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
    HttpClientConfig clientConfig;
    getBool(clientConfig.sslVerifyHost, HttpClientConfig::sslVerifyHostKey);
    getBool(clientConfig.sslVerifyPeer, HttpClientConfig::sslVerifyPeerKey);
    getString(clientConfig.caPath, HttpClientConfig::caPathKey);
    getString(clientConfig.caInfo, HttpClientConfig::caInfoKey);
    getString(clientConfig.caInfoVal, HttpClientConfig::caInfoValKey);
    getBool(clientConfig.proxySslVerifyHost, HttpClientConfig::proxySslVerifyHostKey);
    getBool(clientConfig.proxySslVerifyPeer, HttpClientConfig::proxySslVerifyPeerKey);
    getString(clientConfig.proxyCaPath, HttpClientConfig::proxyCaPathKey);
    getString(clientConfig.proxyCaInfo, HttpClientConfig::proxyCaInfoKey);
    getString(clientConfig.proxyCaInfoVal, HttpClientConfig::proxyCaInfoValKey);
    getString(clientConfig.proxy, HttpClientConfig::proxyKey);
    getString(clientConfig.noProxy, HttpClientConfig::noProxyKey);
    getLong(clientConfig.httpProxyTunnel, HttpClientConfig::httpProxyTunnelKey);
    getLong(clientConfig.connectTimeout, HttpClientConfig::connectTimeoutKey);
    getLong(clientConfig.timeout, HttpClientConfig::timeoutKey);
    getLong(clientConfig.lowSpeedLimit, HttpClientConfig::lowSpeedLimitKey);
    getLong(clientConfig.lowSpeedTime, HttpClientConfig::lowSpeedTimeKey);
    return clientConfig;
}

}  // namespace lsst::qserv::replica
