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
#ifndef LSST_QSERV_HTTP_CLIENT_H
#define LSST_QSERV_HTTP_CLIENT_H

// System headers
#include <functional>
#include <string>
#include <vector>

// Third-party headers
#include "curl/curl.h"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Method.h"

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

/**
 * Class Client is a simple interface for communicating over the HTTP protocol.
 * The implementation of the class invokes a user-supplied callback (lambda) function for
 * each sequence of bytes read from the input stream.
 *
 * Here is an example of using the class to pull a file and dump its content on
 * to the standard output stream:
 * @code
 *   Client reader("GET", "http://my.host.domain/data/chunk_0.txt");
 *   reader.read([](char const* buf, size_t size) {
 *       std::cout << str::string(buf, size);
 *   });
 * @code
 * Another example illustrates how to use the class for interacting with a REST service
 * that excepts a JSON object in the "POST" request's body and returns another JSON
 * object as a result.
 * @code
 *   auto const request = nlohmann::json::object({
 *       {"name", "A"},
 *       {"color_id", 123}
 *   });
 *   std::vector<std::string> const headers = {"Content-Type: application/json"};
 *   Client client("POST", "http://svc.domain.net/create", request.dump(), headers);
 *   nlohmann::json const result = client.readAsJson();
 * @code
 *
 */
class Client {
public:
    /// The function type for notifications on each record retrieved from the input stream.
    typedef std::function<void(char const*, size_t)> CallbackType;

    // No copy semantics for this class.
    Client() = delete;
    Client(Client const&) = delete;
    Client& operator=(Client const&) = delete;

    /// Non-trivial destructor is needed to free up allocated resources.
    ~Client();

    /**
     * @param method An HTTP method.
     * @param url A location of the remote resoure.
     * @param data Optional data to be sent with a request (depends on the HTTP headers).
     * @param headers Optional HTTP headers to be send with a request.
     * @param clientConfig Optional configuration parameters of the reader.
     */
    Client(http::Method method, std::string const& url, std::string const& data = std::string(),
           std::vector<std::string> const& headers = std::vector<std::string>(),
           ClientConfig const& clientConfig = ClientConfig());

    /**
     * Begin processing a request. The whole content of the remote data source
     * refferred to by a URL passed into the constructor will be read. A callback
     * for each record retrieved from the input stream will be called.
     *
     * @note This method is safe to be called multiple times.
     * @param onDataRead A pointer to a function to be called on each sequence of bytes
     *   read from an input stream.
     * @throw std::invalid_argument If a non-valid (empty) function pointer was provided.
     * @throw std::runtime_error For any errors encountered during data retrieval.
     */
    void read(CallbackType const& onDataRead);

    /**
     * @brief Send a request to a service that is expected to return a JSON object.
     * @note The result of the operation is expected to fit into the process memory.
     *   Memory allocation exception will be thrown for large results. The method
     *   may also throw JSON-specific exception in case if the result couldn't be
     *   interpreted as a valid JSON object.
     * @return nlohmann::json a response as a JSON object.
     * @throw std::runtime_error For any errors encountered during data retrieval.
     */
    nlohmann::json readAsJson();

private:
    /**
     * Set connection options as requested in the client configuration.
     * @see _errorChecked for exceptions thrown by the method.
     */
    void _setConnOptions();

    /**
     * Set SSL/TLS certificate as requested in the client configuration.
     * @see _errorChecked for exceptions thrown by the method.
     */
    void _setSslCertOptions();

    /**
     * Set proxy options as requested in the client configuration.
     * @see _errorChecked for exceptions thrown by the method.
     */
    void _setProxyOptions();

    /**
     * Check for an error condition.
     *
     * @param scope A location from which the method was called (used for error reporting).
     * @param errnum A result reported by the CURL library function.
     * @throw std::runtime_error If the error-code is not CURL_OK.
     */
    void _errorChecked(std::string const& scope, CURLcode errnum);

    /**
     * This method is invoked by function forwardToClient() on each chunk of data
     * reported by CURL while streaming in data from a remote server.
     *
     * @param ptr A pointer to the beginning of the data buffer.
     * @param nchars The number of characters in the buffer.
     */
    void _store(char const* ptr, size_t nchars);

    /**
     * The non-member callback function is used for pushing chunks of data retrieved from
     * an input stream managed by libcurl into the class's method _store().
     *
     * See the implementation of the class for further details on the function.
     * See the documentation on lincurl C API for an explanation of
     * the function's parameters.
     */
    friend std::size_t forwardToClient(char* ptr, std::size_t size, std::size_t nmemb, void* client);

    // Input parameters

    http::Method const _method;
    std::string const _url;
    std::string const _data;
    std::vector<std::string> const _headers;
    ClientConfig const _clientConfig;

    CallbackType _onDataRead;  ///< set by method read() before pulling the data

    // Cached members
    CURL* _hcurl = nullptr;
    curl_slist* _hlist = nullptr;
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_CLIENT_H
