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
#include "qhttp/Status.h"

using namespace std;

namespace lsst::qserv::qhttp {
string status2str(Status status) {
    auto const code = "(" + to_string(static_cast<unsigned int>(status)) + ")";
    switch (status) {
        case Status::STATUS_CONTINUE:
            return "STATUS_CONTINUE" + code;
        case Status::STATUS_SWITCH_PROTOCOL:
            return "STATUS_SWITCH_PROTOCOL" + code;
        case Status::STATUS_PROCESSING:
            return "STATUS_PROCESSING" + code;
        case Status::STATUS_OK:
            return "STATUS_OK" + code;
        case Status::STATUS_CREATED:
            return "STATUS_CREATED" + code;
        case Status::STATUS_ACCEPTED:
            return "STATUS_ACCEPTED" + code;
        case Status::STATUS_NON_AUTHORATIVE_INFO:
            return "STATUS_NON_AUTHORATIVE_INFO" + code;
        case Status::STATUS_NO_CONTENT:
            return "STATUS_NO_CONTENT" + code;
        case Status::STATUS_RESET_CONTENT:
            return "STATUS_RESET_CONTENT" + code;
        case Status::STATUS_PARTIAL_CONTENT:
            return "STATUS_PARTIAL_CONTENT" + code;
        case Status::STATUS_MULTI_STATUS:
            return "STATUS_MULTI_STATUS" + code;
        case Status::STATUS_ALREADY_REPORTED:
            return "STATUS_ALREADY_REPORTED" + code;
        case Status::STATUS_IM_USED:
            return "STATUS_IM_USED" + code;
        case Status::STATUS_MULTIPLE_CHOICES:
            return "STATUS_MULTIPLE_CHOICES" + code;
        case Status::STATUS_MOVED_PERM:
            return "STATUS_MOVED_PERM" + code;
        case Status::STATUS_FOUND:
            return "STATUS_FOUND" + code;
        case Status::STATUS_SEE_OTHER:
            return "STATUS_SEE_OTHER" + code;
        case Status::STATUS_NOT_MODIFIED:
            return "STATUS_NOT_MODIFIED" + code;
        case Status::STATUS_USE_PROXY:
            return "STATUS_USE_PROXY" + code;
        case Status::STATUS_TEMP_REDIRECT:
            return "STATUS_TEMP_REDIRECT" + code;
        case Status::STATUS_PERM_REDIRECT:
            return "STATUS_PERM_REDIRECT" + code;
        case Status::STATUS_BAD_REQ:
            return "STATUS_BAD_REQ" + code;
        case Status::STATUS_UNAUTHORIZED:
            return "STATUS_UNAUTHORIZED" + code;
        case Status::STATUS_PAYMENT_REQUIRED:
            return "STATUS_PAYMENT_REQUIRED" + code;
        case Status::STATUS_FORBIDDEN:
            return "STATUS_FORBIDDEN" + code;
        case Status::STATUS_NOT_FOUND:
            return "STATUS_NOT_FOUND" + code;
        case Status::STATUS_METHOD_NOT_ALLOWED:
            return "STATUS_METHOD_NOT_ALLOWED" + code;
        case Status::STATUS_NON_ACCEPTABLE:
            return "STATUS_NON_ACCEPTABLE" + code;
        case Status::STATUS_PROXY_AUTH_REQUIRED:
            return "STATUS_PROXY_AUTH_REQUIRED" + code;
        case Status::STATUS_REQ_TIMEOUT:
            return "STATUS_REQ_TIMEOUT" + code;
        case Status::STATUS_CONFLICT:
            return "STATUS_CONFLICT" + code;
        case Status::STATUS_GONE:
            return "STATUS_GONE" + code;
        case Status::STATUS_LENGTH_REQUIRED:
            return "STATUS_LENGTH_REQUIRED" + code;
        case Status::STATUS_PRECOND_FAILED:
            return "STATUS_PRECOND_FAILED" + code;
        case Status::STATUS_PAYLOAD_TOO_LARGE:
            return "STATUS_PAYLOAD_TOO_LARGE" + code;
        case Status::STATUS_URI_TOO_LONG:
            return "STATUS_URI_TOO_LONG" + code;
        case Status::STATUS_UNSUPPORTED_MEDIA_TYPE:
            return "STATUS_UNSUPPORTED_MEDIA_TYPE" + code;
        case Status::STATUS_RANGE_NOT_SATISFIABLE:
            return "STATUS_RANGE_NOT_SATISFIABLE" + code;
        case Status::STATUS_FAILED_EXPECT:
            return "STATUS_FAILED_EXPECT" + code;
        case Status::STATUS_MISREDIRECT_REQ:
            return "STATUS_MISREDIRECT_REQ" + code;
        case Status::STATUS_UNPROCESSIBLE:
            return "STATUS_UNPROCESSIBLE" + code;
        case Status::STATUS_LOCKED:
            return "STATUS_LOCKED" + code;
        case Status::STATUS_FAILED_DEP:
            return "STATUS_FAILED_DEP" + code;
        case Status::STATUS_TOO_EARLY:
            return "STATUS_TOO_EARLY" + code;
        case Status::STATUS_UPGRADE_REQUIRED:
            return "STATUS_UPGRADE_REQUIRED" + code;
        case Status::STATUS_PRECOND_REQUIRED:
            return "STATUS_PRECOND_REQUIRED" + code;
        case Status::STATUS_TOO_MANY_REQS:
            return "STATUS_TOO_MANY_REQS" + code;
        case Status::STATUS_INTERNAL_SERVER_ERR:
            return "STATUS_INTERNAL_SERVER_ERR" + code;
        case Status::STATUS_NOT_IMPL:
            return "STATUS_NOT_IMPL" + code;
        case Status::STATUS_BAD_GATEWAY:
            return "STATUS_BAD_GATEWAY" + code;
        case Status::STATUS_SERVICE_UNAVAIL:
            return "STATUS_SERVICE_UNAVAIL" + code;
        case Status::STATUS_GATEWAY_TIMEOUT:
            return "STATUS_GATEWAY_TIMEOUT" + code;
        case Status::STATUS_UNDSUPPORT_VERSION:
            return "STATUS_UNDSUPPORT_VERSION" + code;
        case Status::STATUS_VARIANT_NEGOTIATES:
            return "STATUS_VARIANT_NEGOTIATES" + code;
        case Status::STATUS_NO_STORAGE:
            return "STATUS_NO_STORAGE" + code;
        case Status::STATUS_LOOP:
            return "STATUS_LOOP" + code;
        case Status::STATUS_NOT_EXTENDED:
            return "STATUS_NOT_EXTENDED" + code;
        case Status::STATUS_NET_AUTH_REQUIRED:
            return "STATUS_NET_AUTH_REQUIRED" + code;
        default:
            return "STATUS_UNKNOWN" + code;
    }
}

}  // namespace lsst::qserv::qhttp
