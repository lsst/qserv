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
#ifndef LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H
#define LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H

// System headers
#include <fstream>
#include <map>
#include <string>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/HttpCzarIngestModuleBase.h"
#include "http/FileUploadModule.h"

// Forward declarations

namespace lsst::qserv::czar::ingest {
class Processor;
}  // namespace lsst::qserv::czar::ingest

namespace lsst::qserv::http {
class ClientConnPool;
}  // namespace lsst::qserv::http

namespace httplib {
class ContentReader;
class Request;
class Response;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarIngestCsvModule implements a handler for processing requests for ingesting
 * user-generated data prodicts via the HTTP-based frontend. The requests are expected to
 * contain CSV data, JSON schema and the relevant parameters in the multipart/form-data body of
 * the request.
 */
class HttpCzarIngestCsvModule : public http::FileUploadModule, public HttpCzarIngestModuleBase {
public:
    static void process(boost::asio::io_service& io_service, std::string const& context,
                        std::string const& tmpDir, httplib::Request const& req, httplib::Response& resp,
                        httplib::ContentReader const& contentReader,
                        std::shared_ptr<http::ClientConnPool> const& clientConnPool,
                        std::shared_ptr<ingest::Processor> const& workerIngestProcessor,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarIngestCsvModule() = delete;
    HttpCzarIngestCsvModule(HttpCzarIngestCsvModule const&) = delete;
    HttpCzarIngestCsvModule& operator=(HttpCzarIngestCsvModule const&) = delete;

    /// Destructor is responsible for cleaning up the temporary files.
    virtual ~HttpCzarIngestCsvModule();

protected:
    virtual std::string context() const final;
    virtual void onStartOfFile(std::string const& name, std::string const& fileName,
                               std::string const& contentType) final;
    virtual void onFileData(char const* data, std::size_t length) final;
    virtual void onEndOfFile() final;
    virtual nlohmann::json onEndOfBody() final;

private:
    HttpCzarIngestCsvModule(boost::asio::io_service& io_service, std::string const& context,
                            std::string const& tmpDir, httplib::Request const& req, httplib::Response& resp,
                            httplib::ContentReader const& contentReader,
                            std::shared_ptr<http::ClientConnPool> const& clientConnPool,
                            std::shared_ptr<ingest::Processor> const& workerIngestProcessor);

    /// Ingest the table data into the Qserv and the Replication/Ingest system in a conetxt of
    /// the given transaction.
    std::map<std::string, std::string> _pushDataToWorkers(std::uint32_t transactionId);

    // Input parameters

    /// The context string for posting messages into the logging stream.
    std::string const _context;

    /// The temporary directory for storing the uploaded files.
    std::string const _tmpDir;

    /// The HTTP connection pool for communications with workers.
    std::shared_ptr<http::ClientConnPool> const _clientConnPool;

    /// The ingest processor for uploading data to workers.
    std::shared_ptr<ingest::Processor> const _workerIngestProcessor;

    // The following parameters are used to store the uploaded files.
    std::string _name;         ///< The name of a file entry that is open ("rows", "schema" or "indexes").
    std::string _csvFileName;  ///< The name of the CSV file in the temporary directory.
    std::ofstream _csvFile;    ///< The output stream for the CSV file.
    std::string _schema;       ///< The schema payload before parsing it into the JSON object.
    std::string _indexes;      ///< The indexes payload before parsing it into the JSON object.

    // The following parameters are parsed from the request body.
    std::string _databaseName;
    std::string _tableName;
    std::string _charsetName;
    std::string _collationName;
    std::string _fieldsTerminatedBy;
    std::string _fieldsEnclosedBy;
    std::string _fieldsEscapedBy;
    std::string _linesTerminatedBy;

    // Ingest statistics
    std::uint32_t _transactionId = 0;  ///< The transaction ID assigned by the Replication/Ingest system.
    std::atomic<std::uint32_t> _numChunks{0};  ///< The total number of chunks ingested.
    std::atomic<std::uint64_t> _numRows{0};    ///< The total number of rows ingested.
    std::atomic<std::uint64_t> _numBytes{0};   ///< The number of bytes found in the input data file/stream.
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARINGESTCSVMODULE_H
