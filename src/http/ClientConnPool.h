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
#ifndef LSST_QSERV_HTTP_CLIENTCONNPOOL_H
#define LSST_QSERV_HTTP_CLIENTCONNPOOL_H

// System headers
#include <mutex>
#include <string>

// Third-party headers
#include "curl/curl.h"

// This header declarations
namespace lsst::qserv::http {
/**
 * Class ClientConnPool is a helper class utilizing the libcurl's context
 * sharing mechanism for building a configurable pool of the TCP connections.
 * Note that this implementation doesn't directly manage connections. The connections
 * are owned and managed by the library itself. A role of the class is to provide
 * a synchronization context for acquring/releasing these connections in the multi-thread
 * environment.
 *
 * The implementation is based on: https://curl.se/libcurl/c/libcurl-share.html
 */
class ClientConnPool {
public:
    /**
     * Initialize the pool
     * @param maxConnections The maximum number of connections allowed in the pool.
     *   If 0 is passed as a value of the parameter then the default pool size
     *   (depends in an implementation of the loibcurl library) will not be changed.
     */
    explicit ClientConnPool(long maxConnections = 0);
    ClientConnPool(ClientConnPool const&) = delete;
    ClientConnPool& operator=(ClientConnPool const&) = delete;

    ~ClientConnPool();

    long maxConnections() const { return _maxConnections; }
    CURLSH* shareCurl() { return _shareCurl; }

private:
    // These callback functions are required by libcurl to allow easy-based instances
    // of the class Client acquire/release connections from the pool.

    static void _share_lock_cb(CURL* handle, curl_lock_data data, curl_lock_access access, void* userptr);
    static void _share_unlock_cb(CURL* handle, curl_lock_data data, curl_lock_access access, void* userptr);

    /**
     * Check for an error condition.
     *
     * @param scope A location from which the method was called (used for error reporting).
     * @param errnum A result reported by the CURL library function.
     * @throw std::runtime_error If the error-code is not CURLSHE_OK.
     */
    void _errorChecked(std::string const& scope, CURLSHcode errnum);

    /// The mutex is shared by all instances of the pool.
    static std::mutex _accessShareCurlMtx;

    long const _maxConnections = 0;
    CURLSH* _shareCurl;
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_CLIENTCONNPOOL_H
