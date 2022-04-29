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
#include "replica/HttpAsyncReqApp.h"

// System headers
#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HttpAsyncReq.h"

using namespace std;
using json = nlohmann::json;

namespace {

string const description =
        "This application sends requests to a Web server over the HTTP/HTTPS protocol"
        " using the asynchronous client API. If option '--file=<file>' is present the result"
        " will be writted to the specified file. Otherwise the content will be printed to"
        " the standard output stream.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

vector<string> const allowedMethods = {"GET", "POST", "PUT", "DELETE"};

string vector2str(vector<std::string> const& v) {
    ostringstream oss;
    copy(v.cbegin(), v.cend(), ostream_iterator<string>(oss, " "));
    return oss.str();
}
}  // namespace

namespace lsst { namespace qserv { namespace replica {

HttpAsyncReqApp::Ptr HttpAsyncReqApp::create(int argc, char* argv[]) {
    return Ptr(new HttpAsyncReqApp(argc, argv));
}

HttpAsyncReqApp::HttpAsyncReqApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().required("url", "The URL to read data from.", _url)
            .option("method", "The HTTP method. Allowed values: " + ::vector2str(::allowedMethods), _method,
                    ::allowedMethods)
            .option("header",
                    "The HTTP header to be sent with a request. Note this test application allows"
                    " only one header. The format of the header is '<key>[:<val>]'.",
                    _header)
            .option("data", "The data to be sent in the body of a request.", _data)
            .option("max-response-data-size",
                    "The maximum size (bytes) of the response body. If a value of the parameter is set"
                    " to 0 then the default limit of 8M imposed by the Boost.Beast library will be assumed.",
                    _maxResponseBodySize)
            .option("expiration-ival-sec",
                    "A timeout to wait before the completion of a request. The expiration timeout includes"
                    " all phases of the request's execution, including establishing a connection"
                    " to the server, sending the request and waiting for the server's response."
                    " If a value of the parameter is set to 0 then no expiration timeout will be"
                    " assumed for the request.",
                    _expirationIvalSec)
            .option("file",
                    "A path to an output file where the response body received from a remote source will"
                    "  be written. This option is ignored if the flag --body is not specified.",
                    _file)
            .flag("result2json",
                  "If specified the flag will cause the application to interpret the response body as"
                  " a JSON object.",
                  _result2json)
            .flag("verbose",
                  "The flag that allows printing the completion status and the response header"
                  " info onto the standard output stream.",
                  _verbose)
            .flag("body",
                  "The flag that allows printing the complete response body. If the --file=<path> option"
                  " is specified then the body will be written into that files. Otherwise it will be"
                  " printed onto the standard output stream.",
                  _body);
}

int HttpAsyncReqApp::runImpl() {
    string const context = "HttpAsyncReqApp::" + string(__func__) + "  ";
    unordered_map<string, string> headers;
    if (!_header.empty()) {
        auto const pos = _header.find(":");
        if (pos == string::npos) {
            headers[_header] = string();
        } else {
            headers[_header.substr(0, pos)] = _header.substr(pos + 1);
        }
    }
    ostream* osPtr = nullptr;
    ofstream fs;
    if (_body) {
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
    boost::asio::io_service io_service;
    auto const ptr = HttpAsyncReq::create(
            io_service, [this, osPtr](auto const& ptr) { this->_dump(ptr, osPtr); }, _method, _url, _data,
            headers, _maxResponseBodySize, _expirationIvalSec);

    ptr->start();
    io_service.run();

    if (nullptr != osPtr) {
        osPtr->flush();
        if (fs.is_open()) fs.close();
    }
    return ptr->state() == HttpAsyncReq::State::FINISHED ? 0 : 1;
}

void HttpAsyncReqApp::_dump(shared_ptr<HttpAsyncReq> const& ptr, ostream* osPtr) const {
    if (_verbose) {
        cout << "Request completion state: " << HttpAsyncReq::state2str(ptr->state())
             << ", error message: " << ptr->errorMessage() << endl;
    }
    if (ptr->state() == HttpAsyncReq::State::FINISHED ||
        ptr->state() == HttpAsyncReq::State::BODY_LIMIT_ERROR) {
        if (_verbose) {
            cout << "  HTTP response code: " << ptr->responseCode() << "\n";
            cout << "  response header:\n";
            for (auto&& elem : ptr->responseHeader()) {
                cout << "    " << elem.first << ": " << elem.second << "\n";
            }
        }
        if (ptr->state() == HttpAsyncReq::State::FINISHED) {
            if (_verbose) {
                cout << "  response body size: " << ptr->responseBodySize() << endl;
            }
            if (nullptr != osPtr) {
                if (_result2json) {
                    *osPtr << json::parse(ptr->responseBody()).dump();
                } else {
                    *osPtr << ptr->responseBody();
                }
            }
        }
        cout.flush();
    }
}

}}}  // namespace lsst::qserv::replica
