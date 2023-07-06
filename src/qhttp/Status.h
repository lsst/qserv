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
#ifndef LSST_QSERV_QSTATUS_H
#define LSST_QSERV_QSTATUS_H

// This header declarations

namespace lsst::qserv::qhttp {

/**
 * The enumeration type for symbolic definitions of the known HTTP status codes.
 */
enum Status : unsigned int {
    STATUS_CONTINUE = 100,
    STATUS_SWITCH_PROTOCOL = 101,
    STATUS_PROCESSING = 102,
    STATUS_OK = 200,
    STATUS_CREATED = 201,
    STATUS_ACCEPTED = 202,
    STATUS_NON_AUTHORATIVE_INFO = 203,
    STATUS_NO_CONTENT = 204,
    STATUS_RESET_CONTENT = 205,
    STATUS_PARTIAL_CONTENT = 206,
    STATUS_MULTI_STATUS = 207,
    STATUS_ALREADY_REPORTED = 208,
    STATUS_IM_USED = 226,
    STATUS_MULTIPLE_CHOICES = 300,
    STATUS_MOVED_PERM = 301,
    STATUS_FOUND = 302,
    STATUS_SEE_OTHER = 303,
    STATUS_NOT_MODIFIED = 304,
    STATUS_USE_PROXY = 305,
    STATUS_TEMP_REDIRECT = 307,
    STATUS_PERM_REDIRECT = 308,
    STATUS_BAD_REQ = 400,
    STATUS_UNAUTHORIZED = 401,
    STATUS_PAYMENT_REQUIRED = 402,
    STATUS_FORBIDDEN = 403,
    STATUS_NOT_FOUND = 404,
    STATUS_METHOD_NOT_ALLOWED = 405,
    STATUS_NON_ACCEPTABLE = 406,
    STATUS_PROXY_AUTH_REQUIRED = 407,
    STATUS_REQ_TIMEOUT = 408,
    STATUS_CONFLICT = 409,
    STATUS_GONE = 410,
    STATUS_LENGTH_REQUIRED = 411,
    STATUS_PRECOND_FAILED = 412,
    STATUS_PAYLOAD_TOO_LARGE = 413,
    STATUS_URI_TOO_LONG = 414,
    STATUS_UNSUPPORTED_MEDIA_TYPE = 415,
    STATUS_RANGE_NOT_SATISFIABLE = 416,
    STATUS_FAILED_EXPECT = 417,
    STATUS_MISREDIRECT_REQ = 421,
    STATUS_UNPROCESSIBLE = 422,
    STATUS_LOCKED = 423,
    STATUS_FAILED_DEP = 424,
    STATUS_UPGRADE_REQUIRED = 426,
    STATUS_PRECOND_REQUIRED = 428,
    STATUS_TOO_MANY_REQS = 429,
    STATUS_REQ_HDR_FIELDS_TOO_LARGE = 431,
    STATUS_INTERNAL_SERVER_ERR = 500,
    STATUS_NOT_IMPL = 501,
    STATUS_BAD_GATEWAY = 502,
    STATUS_SERVICE_UNAVAIL = 503,
    STATUS_GSATEWAY_TIMEOUT = 504,
    STATUS_UNDSUPPORT_VERSION = 505,
    STATUS_VARIANT_NEGOTIATES = 506,
    STATUS_NO_STORAGE = 507,
    STATUS_LOOP = 508,
    STATUS_NOT_EXTENDED = 510,
    STATUS_NET_AUTH_REQUIRED = 511
};

}  // namespace lsst::qserv::qhttp

#endif /* LSST_QSERV_QSTATUS_H */
