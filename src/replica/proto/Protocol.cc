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
#include "replica/proto/Protocol.h"

// System headers
#include <stdexcept>

using namespace std;

namespace lsst::qserv::replica::protocol {

std::string toString(Status status) {
    switch (status) {
        case Status::CREATED:
            return "CREATED";
        case Status::SUCCESS:
            return "SUCCESS";
        case Status::QUEUED:
            return "QUEUED";
        case Status::IN_PROGRESS:
            return "IN_PROGRESS";
        case Status::IS_CANCELLING:
            return "IS_CANCELLING";
        case Status::BAD:
            return "BAD";
        case Status::FAILED:
            return "FAILED";
        case Status::CANCELLED:
            return "CANCELLED";
        default:
            throw logic_error("Unhandled status: " + to_string(static_cast<int>(status)));
    }
}

std::string toString(StatusExt extendedStatus) {
    switch (extendedStatus) {
        case StatusExt::NONE:
            return "NONE";
        case StatusExt::INVALID_PARAM:
            return "INVALID_PARAM";
        case StatusExt::INVALID_ID:
            return "INVALID_ID";
        case StatusExt::FOLDER_STAT:
            return "FOLDER_STAT";
        case StatusExt::FOLDER_CREATE:
            return "FOLDER_CREATE";
        case StatusExt::FILE_STAT:
            return "FILE_STAT";
        case StatusExt::FILE_SIZE:
            return "FILE_SIZE";
        case StatusExt::FOLDER_READ:
            return "FOLDER_READ";
        case StatusExt::FILE_READ:
            return "FILE_READ";
        case StatusExt::FILE_ROPEN:
            return "FILE_ROPEN";
        case StatusExt::FILE_CREATE:
            return "FILE_CREATE";
        case StatusExt::FILE_OPEN:
            return "FILE_OPEN";
        case StatusExt::FILE_RESIZE:
            return "FILE_RESIZE";
        case StatusExt::FILE_WRITE:
            return "FILE_WRITE";
        case StatusExt::FILE_COPY:
            return "FILE_COPY";
        case StatusExt::FILE_DELETE:
            return "FILE_DELETE";
        case StatusExt::FILE_RENAME:
            return "FILE_RENAME";
        case StatusExt::FILE_EXISTS:
            return "FILE_EXISTS";
        case StatusExt::SPACE_REQ:
            return "SPACE_REQ";
        case StatusExt::NO_FOLDER:
            return "NO_FOLDER";
        case StatusExt::NO_FILE:
            return "NO_FILE";
        case StatusExt::NO_ACCESS:
            return "NO_ACCESS";
        case StatusExt::NO_SPACE:
            return "NO_SPACE";
        case StatusExt::FILE_MTIME:
            return "FILE_MTIME";
        case StatusExt::MYSQL_ERROR:
            return "MYSQL_ERROR";
        case StatusExt::LARGE_RESULT:
            return "LARGE_RESULT";
        case StatusExt::NO_SUCH_TABLE:
            return "NO_SUCH_TABLE";
        case StatusExt::NOT_PARTITIONED_TABLE:
            return "NOT_PARTITIONED_TABLE";
        case StatusExt::NO_SUCH_PARTITION:
            return "NO_SUCH_PARTITION";
        case StatusExt::MULTIPLE:
            return "MULTIPLE";
        case StatusExt::OTHER_EXCEPTION:
            return "OTHER_EXCEPTION";
        case StatusExt::FOREIGN_INSTANCE:
            return "FOREIGN_INSTANCE";
        case StatusExt::DUPLICATE_KEY:
            return "DUPLICATE_KEY";
        case StatusExt::CANT_DROP_KEY:
            return "CANT_DROP_KEY";
        default:
            throw logic_error("Unhandled extended status: " + to_string(static_cast<int>(extendedStatus)));
    }
}

string toString(Status status, StatusExt extendedStatus) {
    return toString(status) + "::" + toString(extendedStatus);
}

}  // namespace lsst::qserv::replica::protocol
