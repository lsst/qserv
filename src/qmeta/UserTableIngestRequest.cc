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
#include "qmeta/UserTableIngestRequest.h"

// System headers
#include <stdexcept>

namespace lsst::qserv::qmeta {

std::string UserTableIngestRequest::status2str(Status const status) {
    switch (status) {
        case Status::IN_PROGRESS:
            return "IN_PROGRESS";
        case Status::COMPLETED:
            return "COMPLETED";
        case Status::FAILED:
            return "FAILED";
        case Status::FAILED_LR:
            return "FAILED_LR";
    }
    throw std::logic_error("Unknown status: " + std::to_string(static_cast<int>(status)));
}

UserTableIngestRequest::Status UserTableIngestRequest::str2status(std::string const& statusStr) {
    if (statusStr == "IN_PROGRESS") {
        return UserTableIngestRequest::Status::IN_PROGRESS;
    } else if (statusStr == "COMPLETED") {
        return UserTableIngestRequest::Status::COMPLETED;
    } else if (statusStr == "FAILED") {
        return UserTableIngestRequest::Status::FAILED;
    } else if (statusStr == "FAILED_LR") {
        return UserTableIngestRequest::Status::FAILED_LR;
    }
    throw std::invalid_argument("Unknown status string: '" + statusStr + "'");
}

std::string UserTableIngestRequest::tableType2str(TableType const tableType) {
    switch (tableType) {
        case TableType::FULLY_REPLICATED:
            return "FULLY_REPLICATED";
        case TableType::DIRECTOR:
            return "DIRECTOR";
        case TableType::CHILD:
            return "CHILD";
        case TableType::REF_MATCH:
            return "REF_MATCH";
    }
    throw std::logic_error("Unknown table type: " + std::to_string(static_cast<int>(tableType)));
}

UserTableIngestRequest::TableType UserTableIngestRequest::str2tableType(std::string const& tableTypeStr) {
    if (tableTypeStr == "FULLY_REPLICATED") {
        return UserTableIngestRequest::TableType::FULLY_REPLICATED;
    } else if (tableTypeStr == "DIRECTOR") {
        return UserTableIngestRequest::TableType::DIRECTOR;
    } else if (tableTypeStr == "CHILD") {
        return UserTableIngestRequest::TableType::CHILD;
    } else if (tableTypeStr == "REF_MATCH") {
        return UserTableIngestRequest::TableType::REF_MATCH;
    }
    throw std::invalid_argument("Unknown table type string: '" + tableTypeStr + "'");
}

std::string UserTableIngestRequest::dataFormat2str(DataFormat const dataFormat) {
    switch (dataFormat) {
        case DataFormat::CSV:
            return "CSV";
        case DataFormat::JSON:
            return "JSON";
        case DataFormat::PARQUET:
            return "PARQUET";
    }
    throw std::logic_error("Unknown data format: " + std::to_string(static_cast<int>(dataFormat)));
}

UserTableIngestRequest::DataFormat UserTableIngestRequest::str2dataFormat(std::string const& dataFormatStr) {
    if (dataFormatStr == "CSV") {
        return UserTableIngestRequest::DataFormat::CSV;
    } else if (dataFormatStr == "JSON") {
        return UserTableIngestRequest::DataFormat::JSON;
    } else if (dataFormatStr == "PARQUET") {
        return UserTableIngestRequest::DataFormat::PARQUET;
    }
    throw std::invalid_argument("Unknown data format string: '" + dataFormatStr + "'");
}

nlohmann::json UserTableIngestRequest::toJson() const {
    auto result = nlohmann::json::object({{"id", id},
                                          {"status", status2str(status)},
                                          {"begin_time", beginTime},
                                          {"end_time", endTime},
                                          {"delete_time", deleteTime},
                                          {"error", error},
                                          {"database", database},
                                          {"table", table},
                                          {"table_type", tableType2str(tableType)},
                                          {"is_temporary", isTemporary ? 1 : 0},
                                          {"data_format", dataFormat2str(dataFormat)},
                                          {"schema", schema},
                                          {"indexes", indexes},
                                          {"extended", extended},
                                          {"num_chunks", numChunks},
                                          {"num_rows", numRows},
                                          {"num_bytes", numBytes},
                                          {"transaction_id", transactionId}});
    return result;
}

}  // namespace lsst::qserv::qmeta
