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
#ifndef LSST_QSERV_HTTP_CLIENTCONFIG_H
#define LSST_QSERV_HTTP_CLIENTCONFIG_H

// System headers
#include <string>

// Third-party headers
#include "curl/curl.h"

// This header declarations
namespace lsst::qserv::http {

/**
 * Class ClientConfig encapsulates configuration parameters related to 'libcurl'
 * option setter.
 */
class ClientConfig {
public:
    /// The folder where the parameters are stored in the persistent configuration.
    static std::string const category;

    // The protocol and connection options keys

    static std::string const httpVersionKey;     ///< CURLOPT_HTTP_VERSION
    static std::string const bufferSizeKey;      ///< CURLOPT_BUFFERSIZE
    static std::string const maxConnectsKey;     ///< CURLOPT_MAXCONNECTS
    static std::string const connectTimeoutKey;  ///< CURLOPT_CONNECTTIMEOUT
    static std::string const timeoutKey;         ///< CURLOPT_TIMEOUT
    static std::string const lowSpeedLimitKey;   ///< CURLOPT_LOW_SPEED_LIMIT
    static std::string const lowSpeedTimeKey;    ///< CURLOPT_LOW_SPEED_TIME
    static std::string const tcpKeepAliveKey;    ///< CURLOPT_TCP_KEEPALIVE
    static std::string const tcpKeepIdleKey;     ///< CURLOPT_TCP_KEEPIDLE
    static std::string const tcpKeepIntvlKey;    ///< CURLOPT_TCP_KEEPINTVL

    // Keys for the SSL certs of the final data servers

    static std::string const sslVerifyHostKey;  ///< CURLOPT_SSL_VERIFYHOST
    static std::string const sslVerifyPeerKey;  ///< CURLOPT_SSL_VERIFYPEER
    static std::string const caPathKey;         ///< CURLOPT_CAPATH
    static std::string const caInfoKey;         ///< CURLOPT_CAINFO

    /// A value of a cert which would have to be pulled from the configuration
    /// databases placed into a local file (at worker) be set with 'CURLOPT_CAINFO'.
    /// This option is used if it's impossible to preload required certificates
    /// at workers, or make them directly readable by worker's ingest services otherwise.
    static std::string const caInfoValKey;

    // Configuration parameters of the intermediate proxy servers

    static std::string const proxyKey;               ///< CURLOPT_PROXY
    static std::string const noProxyKey;             ///< CURLOPT_NOPROXY
    static std::string const httpProxyTunnelKey;     ///< CURLOPT_HTTPPROXYTUNNEL
    static std::string const proxySslVerifyHostKey;  ///< CURLOPT_PROXY_SSL_VERIFYHOST
    static std::string const proxySslVerifyPeerKey;  ///< CURLOPT_PROXY_SSL_VERIFYPEER
    static std::string const proxyCaPathKey;         ///< CURLOPT_PROXY_CAPATH
    static std::string const proxyCaInfoKey;         ///< CURLOPT_PROXY_CAINFO

    /// A value of a cert which would have to be pulled from the configuration
    /// databases placed into a local file (at worker) be set with 'CURLOPT_PROXY_CAINFO'.
    /// This option is used if it's impossible to preload required certificates
    /// at workers, or make them directly readable by worker's ingest services otherwise.
    static std::string const proxyCaInfoValKey;

    /// The concurrency limit for the number of the asynchronous requests
    /// to be processes simultaneously.
    /// TODO: Move this parameter to the Replication System's Configuration
    ///       as it doesn't belong here.
    static std::string const asyncProcLimitKey;

    // Objects of this class can be trivially constructed, copied or deleted.
    // The default state of an object corresponds to not having any of the options
    // carried by the class be set when using 'libcurl' API.

    ClientConfig() = default;
    ClientConfig(ClientConfig const&) = default;
    ClientConfig& operator=(ClientConfig const&) = default;
    ~ClientConfig() = default;

    /// The desired version number of the protocol, where CURL_HTTP_VERSION_NONE
    /// corresponds to the default behavior of the library, which depends on a verison
    /// of the library itself.
    /// https://curl.se/libcurl/c/CURLOPT_HTTP_VERSION.html
    long httpVersion = CURL_HTTP_VERSION_NONE;

    long bufferSize = 0;
    long maxConnects = 0;
    bool tcpKeepAlive = false;
    long tcpKeepIdle = 0;
    long tcpKeepIntvl = 0;
    long connectTimeout = 0;
    long timeout = 0;
    long lowSpeedLimit = 0;
    long lowSpeedTime = 0;

    bool sslVerifyHost = true;
    bool sslVerifyPeer = true;
    std::string caPath;
    std::string caInfo;
    std::string caInfoVal;

    std::string proxy;
    std::string noProxy;
    long httpProxyTunnel = 0;
    bool proxySslVerifyHost = true;
    bool proxySslVerifyPeer = true;
    std::string proxyCaPath;
    std::string proxyCaInfo;
    std::string proxyCaInfoVal;

    unsigned int asyncProcLimit = 0;  ///< Zero corresponds to the default behavior (no limit)
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_CLIENTCONFIG_H
