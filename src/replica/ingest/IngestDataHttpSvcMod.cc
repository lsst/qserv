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
#include "replica/ingest/IngestDataHttpSvcMod.h"

// Qserv header
#include "http/BinaryEncoding.h"
#include "http/Exceptions.h"
#include "http/Method.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/config/Configuration.h"
#include "replica/services/DatabaseServices.h"
#include "replica/util/Csv.h"
#include "util/String.h"

// System headers
#include <errno.h>
#include <stdexcept>
#include <vector>

using namespace std;
using json = nlohmann::json;
namespace qhttp = lsst::qserv::qhttp;
namespace util = lsst::qserv::util;

namespace {
/// @return requestor's IP address
string senderIpAddr(shared_ptr<qhttp::Request> const& req) {
    ostringstream ss;
    ss << req->remoteAddr.address();
    return ss.str();
}

/// These keywords are found in all known binary columns types of MySQL.
vector<string> const binColTypePatterns = {"BIT", "BINARY", "BLOB"};

/**
 * @param type The column type name of.
 * @return 'true' if the type represents the binary column type in MySQL.
 */
bool isBinaryColumnType(string const& type) {
    string const typeUpperCase = util::String::toUpper(type);
    for (string const& pattern : binColTypePatterns) {
        if (string::npos != typeUpperCase.find(pattern)) return true;
    }
    return false;
}

}  // namespace

namespace lsst::qserv::replica {

void IngestDataHttpSvcMod::process(ServiceProvider::Ptr const& serviceProvider, string const& workerName,
                                   shared_ptr<qhttp::Request> const& req,
                                   shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                   http::AuthType const authType) {
    IngestDataHttpSvcMod module(serviceProvider, workerName, req, resp);
    module.execute(subModuleName, authType);
}

IngestDataHttpSvcMod::IngestDataHttpSvcMod(ServiceProvider::Ptr const& serviceProvider,
                                           string const& workerName, shared_ptr<qhttp::Request> const& req,
                                           shared_ptr<qhttp::Response> const& resp)
        : http::QhttpModule(serviceProvider->authKey(), serviceProvider->adminAuthKey(), req, resp),
          IngestFileSvc(serviceProvider, workerName) {}

string IngestDataHttpSvcMod::context() const { return "INGEST-DATA-HTTP-SVC "; }

json IngestDataHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    if (subModuleName == "SYNC-PROCESS-DATA") return _syncProcessData();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json IngestDataHttpSvcMod::_syncProcessData() {
    debug(__func__);
    checkApiVersion(__func__, 35);

    auto const context_ = context() + __func__;
    auto const config = serviceProvider()->config();
    auto const databaseServices = serviceProvider()->databaseServices();

    // Fill out parameters in the contribution descriptor. This information is needed
    // for bookeeping and monitoring purposes. The descriptor's state will be kept
    // updated in the Replication/Ingest's database as the contribution processing
    // will be happening.
    _contrib.transactionId = body().required<TransactionId>("transaction_id");
    _contrib.table = body().required<string>("table");
    _contrib.chunk = body().required<unsigned int>("chunk");
    _contrib.isOverlap = body().required<int>("overlap") != 0;
    _contrib.worker = workerName();

    // To indicate the JSON-formatted data were streamed directly into the service
    _contrib.url = "data-json://" + ::senderIpAddr(req()) + "/";
    _contrib.charsetName =
            body().optional<string>("charset_name", config->get<string>("worker", "ingest-charset-name"));

    // Note the double quotes enforced around the fields. This is compatible with
    // the JSON way for packaging strings.
    _contrib.dialectInput.fieldsEnclosedBy = R"(")";

    // Retries are allowed before an attemp to load data into MySQL. When such attempt
    // is made the persistent state of the destination table is supposed to be changed.
    _contrib.retryAllowed = true;

    // This parameters sets a limit foe the number of warnings (should there be any)
    // reported by MySQL after contribution loading attempt. Warnings is an important
    // mechanism for debugging problems with the ingested data.
    _contrib.maxNumWarnings = body().optional<unsigned int>(
            "max_num_warnings", config->get<unsigned int>("worker", "loader-max-warnings"));

    // This is needed for decoding values of the binary columns should they be present
    // in the table schema.
    http::BinaryEncodingMode const binaryEncodingMode =
            http::parseBinaryEncoding(body().optional<string>("binary_encoding", "hex"));

    // Rows are expected to be supplied in the JSON array
    if (!body().has("rows")) {
        throw http::Error(context_, "a collection of rows is missing in the request");
    }
    json const& rows = body().objJson.at("rows");
    if (!rows.is_array()) {
        throw http::Error(context_, "a collection of rows found in the request is not the JSON array");
    }
    if (rows.empty()) {
        throw http::Error(context_, "a collection of rows in the request is empty");
    }

    debug(__func__, "transaction_id: " + to_string(_contrib.transactionId));
    debug(__func__, "table: '" + _contrib.table + "'");
    debug(__func__, "chunk: " + to_string(_contrib.chunk));
    debug(__func__, "overlap: " + string(_contrib.isOverlap ? "1" : "0"));
    debug(__func__, "charset_name: '" + _contrib.charsetName + "'");
    debug(__func__, "max_num_warnings: " + to_string(_contrib.maxNumWarnings));
    debug(__func__, "binary_encoding: '" + http::binaryEncoding2string(binaryEncodingMode) + "'");
    debug(__func__, "rows.size: " + to_string(rows.size()));

    // Attempts to pass invalid transaction identifiers or tables are not recorded
    // as transaction contributions in the persistent state of the Replication/Ingest
    // system since it's impossible to determine a context of these operations.
    // The following operations will throw exceptions should any problems with
    // validation a context of the request will be encountered.
    TransactionInfo const trans = databaseServices->transaction(_contrib.transactionId);
    _contrib.database = trans.database;

    DatabaseInfo const database = config->databaseInfo(_contrib.database);
    TableInfo const table = database.findTable(_contrib.table);

    // Scan table schema for the binary columns and build 0-based index.
    // The index will be required for decoding the input data of the binary columns.
    //
    // NOTES:
    // - The transaction identifier column will not be added to the index since it's
    //   a special column added by the Ingest system. The column is not supposed to be
    //   known to (or used by) the ingest workflows.
    // - The index size will be also used for validating sizes of the input rows.
    bool const failed = true;
    if (table.columns.empty() || (table.columns.front().name != "qserv_trans_id")) {
        _contrib.error = "incomplete or missing table schema";
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(context_);
        throw http::Error(context_, _contrib.error);
    }
    vector<bool> isBinary;
    for (auto const& coldef : table.columns) {
        if (coldef.name == "qserv_trans_id") continue;
        isBinary.push_back(::isBinaryColumnType(coldef.type));
    }

    // Make sure the transaction is in the rigth state.
    if (trans.state != TransactionInfo::State::STARTED) {
        _contrib.error = "transactionId=" + to_string(_contrib.transactionId) + " is not active";
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(context_);
        throw http::Error(context_, _contrib.error);
    }

    // Register the validated contribution and mark it as started.
    csv::Dialect const dialect(_contrib.dialectInput);
    _contrib = databaseServices->createdTransactionContrib(_contrib);
    try {
        _contrib.tmpFile = openFile(_contrib.transactionId, _contrib.table, dialect, _contrib.charsetName,
                                    _contrib.chunk, _contrib.isOverlap);
        _contrib = databaseServices->startedTransactionContrib(_contrib);
    } catch (exception const& ex) {
        _contrib.systemError = errno;
        _contrib.error = ex.what();
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        _failed(context_);
        throw http::Error(context_, _contrib.error);
    }

    // Optimized quote handling for the fields enclosed by the quotes.
    bool const quotedFields = dialect.fieldsEnclosedBy() != '\0';

    // The storage overhead for the transaction identifier prepended at each row.
    // The number is used for estimating and reporting the overall number of bytes in
    // the input contribution. The overhead includes optional quotes and the field terminator.
    size_t const numBytesInTransactionId = sizeof(uint32_t) + (quotedFields ? 2 : 0) + 1;

    // Begin reading, validating and transforming the input data into a valid CSV stream.
    // Note using the string as a buffer. To reduce repeated memory allocations/deallocations
    // when processing rows the algorithm assumes that the capacity of the string is automatically
    // increased to fit the whole row on the first iteraton of the loop. After that the string
    // buffer is expected stay like that for the rest of the data extracton (see a note below on
    // the string clear before processing each row).
    string row;
    for (size_t rowIdx = 0; rowIdx < rows.size(); ++rowIdx) {
        json const& jsonRow = rows[rowIdx];

        // C++ doesn't require the string clear method to keep the previously allocated
        // buffers. The following operations ensures that the amount of memory allocated
        // during the previous iteration (if any) would stay the same.
        size_t const capacity = row.capacity();
        row.clear();
        row.reserve(capacity);

        // These tests would prevent a problem with the input data before making an actual
        // table loading attempt.
        if (!jsonRow.is_array()) {
            _contrib.error = "a row found in the request is not the JSON array";
            _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
            _failed(context_);
            throw http::Error(context_, _contrib.error);
        }
        if (jsonRow.size() != isBinary.size()) {
            _contrib.error = "the row size in the request doesn't match the table schema";
            _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
            _failed(context_);
            throw http::Error(context_, _contrib.error);
        }

        // Extract and process columns
        for (size_t colIdx = 0; colIdx < jsonRow.size(); ++colIdx) {
            json const& jsonColumn = jsonRow[colIdx];
            if (colIdx != 0) row.push_back(dialect.fieldsTerminatedBy());
            if (quotedFields) row.push_back(dialect.fieldsEnclosedBy());
            if (isBinary[colIdx]) {
                switch (binaryEncodingMode) {
                    case http::BinaryEncodingMode::HEX:
                        row.append(_translateHexString(context_, jsonColumn, rowIdx, colIdx));
                        break;
                    case http::BinaryEncodingMode::B64:
                        row.append(_translateBase64String(context_, jsonColumn, rowIdx, colIdx));
                        break;
                    case http::BinaryEncodingMode::ARRAY: {
                        u8string const str = _translateByteArray(context_, jsonColumn, rowIdx, colIdx);
                        row.append(reinterpret_cast<char const*>(str.data()), str.size());
                        break;
                    }
                    default:
                        _contrib.error = "unsupported binary encoding mode '" +
                                         http::binaryEncoding2string(binaryEncodingMode) + "'";
                        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
                        _failed(context_);
                        throw http::Error(context_, _contrib.error);
                }
            } else {
                row.append(_translatePrimitiveType(context_, jsonColumn, rowIdx, colIdx));
            }
            if (quotedFields) row.push_back(dialect.fieldsEnclosedBy());
        }
        row.push_back(dialect.linesTerminatedBy());
        try {
            writeRowIntoFile(row.data(), row.size());
            _contrib.numRows++;
            _contrib.numBytes += numBytesInTransactionId + row.size();
        } catch (exception const& ex) {
            _contrib.error = "failed to write the row into the temporary file at row " + to_string(rowIdx) +
                             ", ex: " + string(ex.what());
            _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
            _failed(context_);
            throw http::Error(context_, _contrib.error);
        }
    }

    // Report that processing of the input data and preparing the contribution file is over.
    _contrib = databaseServices->readTransactionContrib(_contrib);

    // Begin making irreversible changes to the destination table.
    _contrib.retryAllowed = false;
    try {
        loadDataIntoTable(_contrib.maxNumWarnings);
        _contrib.numWarnings = numWarnings();
        _contrib.warnings = warnings();
        _contrib.numRowsLoaded = numRowsLoaded();
        _contrib = databaseServices->loadedTransactionContrib(_contrib);
        closeFile();
    } catch (exception const& ex) {
        _contrib.error = "MySQL load failed, ex: " + string(ex.what());
        _contrib.systemError = errno;
        databaseServices->loadedTransactionContrib(_contrib, failed);
        _failed(context_);
        throw http::Error(context_, _contrib.error);
    }
    return json::object({{"contrib", _contrib.toJson()}});
}

string IngestDataHttpSvcMod::_translateHexString(string const& context_, json const& jsonColumn,
                                                 size_t rowIdx, size_t colIdx) {
    if (jsonColumn.is_string()) {
        try {
            return util::String::fromHex(jsonColumn.get<string>());
        } catch (exception const& ex) {
            _contrib.error = "failed to decode a value of the '" +
                             http::binaryEncoding2string(http::BinaryEncodingMode::HEX) +
                             "' binary encoded column at row " + to_string(rowIdx) + " and column " +
                             to_string(colIdx) + ", ex: " + string(ex.what());
        }
    } else {
        _contrib.error = "unsupported type name '" + string(jsonColumn.type_name()) + "' found at row " +
                         to_string(rowIdx) + " and column " + to_string(colIdx) +
                         " where the string type was expected";
    }
    bool const failed = true;
    _contrib = serviceProvider()->databaseServices()->startedTransactionContrib(_contrib, failed);
    _failed(context_);
    throw http::Error(context_, _contrib.error);
}

string IngestDataHttpSvcMod::_translateBase64String(string const& context_, json const& jsonColumn,
                                                    size_t rowIdx, size_t colIdx) {
    if (jsonColumn.is_string()) {
        try {
            return util::String::fromBase64(jsonColumn.get<string>());
        } catch (exception const& ex) {
            _contrib.error = "failed to decode a value of the '" +
                             http::binaryEncoding2string(http::BinaryEncodingMode::B64) +
                             "' binary encoded column at row " + to_string(rowIdx) + " and column " +
                             to_string(colIdx) + ", ex: " + string(ex.what());
        }
    } else {
        _contrib.error = "unsupported type name '" + string(jsonColumn.type_name()) + "' found at row " +
                         to_string(rowIdx) + " and column " + to_string(colIdx) +
                         " where the string type was expected";
    }
    bool const failed = true;
    _contrib = serviceProvider()->databaseServices()->startedTransactionContrib(_contrib, failed);
    _failed(context_);
    throw http::Error(context_, _contrib.error);
}

u8string IngestDataHttpSvcMod::_translateByteArray(string const& context_, json const& jsonColumn,
                                                   size_t rowIdx, size_t colIdx) {
    if (jsonColumn.is_array()) {
        try {
            // An array of unsigned 8-bit numbers is expected here.
            return jsonColumn.get<u8string>();
        } catch (exception const& ex) {
            _contrib.error = "failed to decode a value of the '" +
                             http::binaryEncoding2string(http::BinaryEncodingMode::ARRAY) +
                             "' binary encoded column at row " + to_string(rowIdx) + " and column " +
                             to_string(colIdx) + ", ex: " + string(ex.what());
        }
    } else {
        _contrib.error = "unsupported type name '" + string(jsonColumn.type_name()) + "' found at row " +
                         to_string(rowIdx) + " and column " + to_string(colIdx) +
                         " where the string type was expected";
    }
    bool const failed = true;
    _contrib = serviceProvider()->databaseServices()->startedTransactionContrib(_contrib, failed);
    _failed(context_);
    throw http::Error(context_, _contrib.error);
}

string IngestDataHttpSvcMod::_translatePrimitiveType(string const& context_, json const& jsonColumn,
                                                     size_t rowIdx, size_t colIdx) {
    if (jsonColumn.is_boolean()) {
        return string(jsonColumn.get<bool>() ? "1" : "0");
    } else if (jsonColumn.is_number_float()) {
        return to_string(jsonColumn.get<double>());
    } else if (jsonColumn.is_number_unsigned()) {
        return to_string(jsonColumn.get<uint64_t>());
    } else if (jsonColumn.is_number_integer()) {
        return to_string(jsonColumn.get<int64_t>());
    } else if (jsonColumn.is_string()) {
        return jsonColumn.get<string>();
    } else {
        _contrib.error = "unsupported type name '" + string(jsonColumn.type_name()) + "' found at row " +
                         to_string(rowIdx) + " and column " + to_string(colIdx) +
                         " where the boolean, numeric or string type was expected";
    }
    bool const failed = true;
    _contrib = serviceProvider()->databaseServices()->startedTransactionContrib(_contrib, failed);
    _failed(context_);
    throw http::Error(context_, _contrib.error);
}

void IngestDataHttpSvcMod::_failed(string const& context_) {
    error(context_, _contrib.error);
    closeFile();
}

}  // namespace lsst::qserv::replica
