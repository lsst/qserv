

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
#include "replica/HttpClientApp.h"

// System headers
#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

// Third-party headers
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

namespace {

string const description =
        "This application sends requests to a Web server over the HTTP/HTTPS protocol."
        " If option '--file=<file>' is present the result will be writted to the"
        " specified file. Otherwise the content will be printed to the standard output stream.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

}  // namespace

namespace lsst::qserv::replica {

HttpClientApp::Ptr HttpClientApp::create(int argc, char* argv[]) {
    return Ptr(new HttpClientApp(argc, argv));
}

HttpClientApp::HttpClientApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().required("url", "The URL to read data from.", _url);
    parser().option("method", "The HTTP method. Allowed values: GET, POST, PUT, DELETE.", _method);
    parser().option("header",
                    "The HTTP header to be sent with a request. Note this application allows "
                    "only one header.",
                    _header);
    parser().option("data", "The data to be sent in the body of a request.", _data);
    parser().reversedFlag("no-ssl-verify-host",
                          "The flag that disables verifying the certificate's name against host.",
                          _clientConfig.sslVerifyHost);
    parser().reversedFlag("no-ssl-verify-peer",
                          "The flag that disables verifying the peer's SSL certificate.",
                          _clientConfig.sslVerifyPeer);
    parser().option("ca-path",
                    "A path to a directory holding CA certificates to verify the peer with. "
                    "This option is ignored if flag --no-ssl-verify-peer is specified.",
                    _clientConfig.caPath);
    parser().option("ca-info",
                    "A path to a Certificate Authority (CA) bundle to verify the peer with. "
                    "This option is ignored if flag --no-ssl-verify-peer is specified.",
                    _clientConfig.caInfo);
    parser().reversedFlag("no-proxy-ssl-verify-host",
                          "The flag that disables verifying the certificate's name against proxy's host.",
                          _clientConfig.proxySslVerifyHost);
    parser().reversedFlag("no-proxy-ssl-verify-peer",
                          "The flag that disables verifying the proxy's SSL certificate.",
                          _clientConfig.proxySslVerifyPeer);
    parser().option("proxy-ca-path",
                    "A path to a directory holding CA certificates to verify the proxy with. "
                    "This option is ignored if flag --no-proxy-ssl-verify-peer is specified.",
                    _clientConfig.proxyCaPath);
    parser().option("proxy-ca-info",
                    "A path to a Certificate Authority (CA) bundle to verify the proxy with. "
                    "This option is ignored if flag --no-proxy-ssl-verify-peer is specified.",
                    _clientConfig.proxyCaInfo);
    parser().option("connect-timeout",
                    "Timeout for the connect phase. It should contain the maximum time in seconds that "
                    "you allow the connection phase to the server to take. This only limits the "
                    "connection phase, it has no impact once it has connected. Set to zero to switch "
                    "to the default built-in connection timeout - 300 seconds.",
                    _clientConfig.connectTimeout);
    parser().option("timeout",
                    "Set maximum time the request is allowed to take. Pass a long as parameter "
                    "containing timeout - the maximum time in seconds that you allow the libcurl "
                    "transfer operation to take. Normally, name lookups can take a considerable "
                    "time and limiting operations risk aborting perfectly normal operations.",
                    _clientConfig.timeout);
    parser().option("low-speed-limit",
                    "Set low speed limit in bytes per second. Pass a long as parameter. It contains "
                    "the average transfer speed in bytes per second that the transfer should be below "
                    "during --low-speed-time=<seconds> for libcurl to consider it to be too slow and "
                    "abort.",
                    _clientConfig.lowSpeedLimit);
    parser().option("low-speed-time",
                    "Set low speed limit time period. Pass a long as parameter. It contains the time "
                    "in number seconds that the transfer speed should be below the "
                    "--low-speed-limit=<bps> for the library to consider it too slow and abort.",
                    _clientConfig.lowSpeedTime);
    parser().option("async-proc-limit",
                    "Set The concurrency limit for the number of the asynchronous requests "
                    "to be processes simultaneously.",
                    _clientConfig.asyncProcLimit);
    parser().option("file",
                    "A path to an output file where the content received from a remote source will "
                    "be written. If the option is not specified then the content will be printed "
                    "onto the standard output stream. This option is ignored if flag --silent is "
                    "specified.",
                    _file);
    parser().flag("result2json",
                  "If specified the flag will cause the application to interpret the result as "
                  "a JSON object.",
                  _result2json);
    parser().flag("silent",
                  "The flag that disables printing or writing the content received from a remote "
                  "source.",
                  _silent);
}

int HttpClientApp::runImpl() {
    string const context = "HttpClientApp::" + string(__func__) + "  ";
    const char* allowedMethods[] = {"GET", "POST", "PUT", "DELETE"};
    if (count(cbegin(allowedMethods), cend(allowedMethods), _method) == 0) {
        throw invalid_argument(context + "unknown HTTP method: " + _method);
    }
    vector<string> headers;
    if (!_header.empty()) headers.push_back(_header);
    ostream* osPtr = nullptr;
    ofstream fs;
    if (!_silent) {
        if (_file.empty()) {
            osPtr = &cout;
        } else {
            fs.open(_file, ios::out | ios::trunc | ios::binary);
            if (!fs.is_open()) {
                throw runtime_error(context + "failed to open/create file: " + _file);
            }
            osPtr = &fs;
        }
    }
    HttpClient reader(_method, _url, _data, headers, _clientConfig);
    if (_result2json) {
        if (nullptr != osPtr) *osPtr << reader.readAsJson() << "\n";
    } else {
        reader.read([&](char const* record, size_t size) {
            if (nullptr != osPtr) *osPtr << string(record, size) << "\n";
        });
    }
    if (nullptr != osPtr) {
        osPtr->flush();
        if (fs.is_open()) fs.close();
    }
    return 0;
}

}  // namespace lsst::qserv::replica
