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
#include "http/ClientConfig.h"

using namespace std;

namespace lsst::qserv::http {

string const ClientConfig::category = "worker-http-file-reader";

string const ClientConfig::httpVersionKey = "CURLOPT_HTTP_VERSION";
string const ClientConfig::bufferSizeKey = "CURLOPT_BUFFERSIZE";
string const ClientConfig::maxConnectsKey = "CURLOPT_MAXCONNECTS";
string const ClientConfig::connectTimeoutKey = "CONNECTTIMEOUT";
string const ClientConfig::timeoutKey = "TIMEOUT";
string const ClientConfig::lowSpeedLimitKey = "LOW_SPEED_LIMIT";
string const ClientConfig::lowSpeedTimeKey = "LOW_SPEED_TIME";
string const ClientConfig::tcpKeepAliveKey = "CURLOPT_TCP_KEEPALIVE";
string const ClientConfig::tcpKeepIdleKey = "CURLOPT_TCP_KEEPIDLE";
string const ClientConfig::tcpKeepIntvlKey = "CURLOPT_TCP_KEEPINTVL";

string const ClientConfig::sslVerifyHostKey = "SSL_VERIFYHOST";
string const ClientConfig::sslVerifyPeerKey = "SSL_VERIFYPEER";
string const ClientConfig::caPathKey = "CAPATH";
string const ClientConfig::caInfoKey = "CAINFO";
string const ClientConfig::caInfoValKey = "CAINFO_VAL";

string const ClientConfig::proxyKey = "CURLOPT_PROXY";
string const ClientConfig::noProxyKey = "CURLOPT_NOPROXY";
string const ClientConfig::httpProxyTunnelKey = "CURLOPT_HTTPPROXYTUNNEL";
string const ClientConfig::proxySslVerifyHostKey = "PROXY_SSL_VERIFYHOST";
string const ClientConfig::proxySslVerifyPeerKey = "PROXY_SSL_VERIFYPEER";
string const ClientConfig::proxyCaPathKey = "PROXY_CAPATH";
string const ClientConfig::proxyCaInfoKey = "PROXY_CAINFO";
string const ClientConfig::proxyCaInfoValKey = "PROXY_CAINFO_VAL";

string const ClientConfig::asyncProcLimitKey = "ASYNC_PROC_LIMIT";

}  // namespace lsst::qserv::http
