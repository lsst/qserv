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
#include "http/ClientConnPool.h"

// Qserv headers
#include "http/Exceptions.h"

// Standard headers
#include <cassert>

using namespace std;

namespace lsst::qserv::http {

mutex ClientConnPool::_accessShareCurlMtx;

ClientConnPool::ClientConnPool(long maxConnections) : _maxConnections(maxConnections) {
    _shareCurl = curl_share_init();
    assert(_shareCurl != nullptr);
    _errorChecked("curl_share_setopt(CURLSHOPT_LOCKFUNC)",
                  curl_share_setopt(_shareCurl, CURLSHOPT_LOCKFUNC, &ClientConnPool::_share_lock_cb));
    _errorChecked("curl_share_setopt(CURLSHOPT_UNLOCKFUNC)",
                  curl_share_setopt(_shareCurl, CURLSHOPT_UNLOCKFUNC, &ClientConnPool::_share_unlock_cb));
    _errorChecked("curl_share_setopt(CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT)",
                  curl_share_setopt(_shareCurl, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT));
}

ClientConnPool::~ClientConnPool() { curl_share_cleanup(_shareCurl); }

void ClientConnPool::_share_lock_cb(CURL* handle, curl_lock_data data, curl_lock_access access,
                                    void* userptr) {
    ClientConnPool::_accessShareCurlMtx.lock();
}

void ClientConnPool::_share_unlock_cb(CURL* handle, curl_lock_data data, curl_lock_access access,
                                      void* userptr) {
    ClientConnPool::_accessShareCurlMtx.unlock();
}

void ClientConnPool::_errorChecked(string const& scope, CURLSHcode errnum) {
    if (errnum != CURLSHE_OK) {
        string const errorStr = curl_share_strerror(errnum);
        long const httpResponseCode = 0;
        http::raiseRetryAllowedError(scope, " error: '" + errorStr + "', errnum: " + to_string(errnum),
                                     httpResponseCode);
    }
}

}  // namespace lsst::qserv::http
