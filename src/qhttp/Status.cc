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
    switch (status) {
        case Status::STATUS_CONTINUE:
            return "STATUS_CONTINUE(100)";
        case Status::STATUS_SWITCH_PROTOCOL:
            return "STATUS_SWITCH_PROTOCOL(101)";
        case Status::STATUS_PROCESSING:
            return "STATUS_PROCESSING(102)";
        case Status::STATUS_OK:
            return "STATUS_OK(200)";
        case Status::STATUS_CREATED:
            return "STATUS_CREATED(201)";
        case Status::STATUS_ACCEPTED:
            return "STATUS_ACCEPTED(202)";
        case Status::STATUS_NON_AUTHORATIVE_INFO:
            return "STATUS_NON_AUTHORATIVE_INFO(203)";
        case Status::STATUS_NO_CONTENT:
            return "STATUS_NO_CONTENT(204)";
        case Status::STATUS_RESET_CONTENT:
            return "STATUS_RESET_CONTENT(205)";
        case Status::STATUS_PARTIAL_CONTENT:
            return "STATUS_PARTIAL_CONTENT(206)";
        case Status::STATUS_MULTI_STATUS:
            return "STATUS_MULTI_STATUS(207)";
        case Status::STATUS_ALREADY_REPORTED:
            return "STATUS_ALREADY_REPORTED(208)";
        case Status::STATUS_IM_USED:
            return "STATUS_IM_USED(226)";
        case Status::STATUS_MULTIPLE_CHOICES:
            return "STATUS_MULTIPLE_CHOICES(300)";
        case Status::STATUS_MOVED_PERM:
            return "STATUS_MOVED_PERM(301)";
        case Status::STATUS_FOUND:
            return "STATUS_FOUND(302)";
        case Status::STATUS_SEE_OTHER:
            return "STATUS_SEE_OTHER(303)";
        case Status::STATUS_NOT_MODIFIED:
            return "STATUS_NOT_MODIFIED(304)";
        case Status::STATUS_USE_PROXY:
            return "STATUS_USE_PROXY(305)";
        case Status::STATUS_TEMP_REDIRECT:
            return "STATUS_TEMP_REDIRECT(307)";
        case Status::STATUS_PERM_REDIRECT:
            return "STATUS_PERM_REDIRECT(308)";
        case Status::STATUS_BAD_REQ:
            return "STATUS_BAD_REQ(400)";
        case Status::STATUS_UNAUTHORIZED:
            return "STATUS_UNAUTHORIZED(401)";
        case Status::STATUS_PAYMENT_REQUIRED:
            return "STATUS_PAYMENT_REQUIRED(402)";
        case Status::STATUS_FORBIDDEN:
            return "STATUS_FORBIDDEN(403)";
        case Status::STATUS_NOT_FOUND:
            return "STATUS_NOT_FOUND(404)";
        case Status::STATUS_METHOD_NOT_ALLOWED:
            return "STATUS_METHOD_NOT_ALLOWED(405)";
        case Status::STATUS_NON_ACCEPTABLE:
            return "STATUS_NON_ACCEPTABLE(406)";
        case Status::STATUS_PROXY_AUTH_REQUIRED:
            return "STATUS_PROXY_AUTH_REQUIRED(407)";
        case Status::STATUS_REQ_TIMEOUT:
            return "STATUS_REQ_TIMEOUT(408)";
        case Status::STATUS_CONFLICT:
            return "STATUS_CONFLICT(409)";
        case Status::STATUS_GONE:
            return "STATUS_GONE(410)";
        case Status::STATUS_LENGTH_REQUIRED:
            return "STATUS_LENGTH_REQUIRED(411)";
        case Status::STATUS_PRECOND_FAILED:
            return "STATUS_PRECOND_FAILED(412)";
        case Status::STATUS_PAYLOAD_TOO_LARGE:
            return "STATUS_PAYLOAD_TOO_LARGE(413)";
        case Status::STATUS_URI_TOO_LONG:
            return "STATUS_URI_TOO_LONG(414)";
        case Status::STATUS_UNSUPPORTED_MEDIA_TYPE:
            return "STATUS_UNSUPPORTED_MEDIA_TYPE(415)";
        case Status::STATUS_RANGE_NOT_SATISFIABLE:
            return "STATUS_RANGE_NOT_SATISFIABLE(416)";
        case Status::STATUS_FAILED_EXPECT:
            return "STATUS_FAILED_EXPECT(417)";
        case Status::STATUS_MISREDIRECT_REQ:
            return "STATUS_MISREDIRECT_REQ(421)";
        case Status::STATUS_UNPROCESSIBLE:
            return "STATUS_UNPROCESSIBLE(422)";
        case Status::STATUS_LOCKED:
            return "STATUS_LOCKED(423)";
        case Status::STATUS_FAILED_DEP:
            return "STATUS_FAILED_DEP(424)";
        case Status::STATUS_TOO_EARLY:
            return "STATUS_TOO_EARLY(425)";
        case Status::STATUS_UPGRADE_REQUIRED:
            return "STATUS_UPGRADE_REQUIRED(426)";
        case Status::STATUS_PRECOND_REQUIRED:
            return "STATUS_PRECOND_REQUIRED(428)";
        case Status::STATUS_TOO_MANY_REQS:
            return "STATUS_TOO_MANY_REQS(429)";
        case Status::STATUS_INTERNAL_SERVER_ERR:
            return "STATUS_INTERNAL_SERVER_ERR(500)";
        case Status::STATUS_NOT_IMPL:
            return "STATUS_NOT_IMPL(501)";
        case Status::STATUS_BAD_GATEWAY:
            return "STATUS_BAD_GATEWAY(502)";
        case Status::STATUS_SERVICE_UNAVAIL:
            return "STATUS_SERVICE_UNAVAIL(503)";
        case Status::STATUS_GATEWAY_TIMEOUT:
            return "STATUS_GATEWAY_TIMEOUT(504)";
        case Status::STATUS_UNDSUPPORT_VERSION:
            return "STATUS_UNDSUPPORT_VERSION(505)";
        case Status::STATUS_VARIANT_NEGOTIATES:
            return "STATUS_VARIANT_NEGOTIATES(506)";
        case Status::STATUS_NO_STORAGE:
            return "STATUS_NO_STORAGE(507)";
        case Status::STATUS_LOOP:
            return "STATUS_LOOP(508)";
        case Status::STATUS_NOT_EXTENDED:
            return "STATUS_NOT_EXTENDED(510)";
        case Status::STATUS_NET_AUTH_REQUIRED:
            return "STATUS_NET_AUTH_REQUIRED(511)";
        default:
            return "STATUS_UNKNOWN(" + to_string(static_cast<unsigned int>(status)) + ")";
    }
}

}  // namespace lsst::qserv::qhttp
