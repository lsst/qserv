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
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <unordered_map>

// Third-party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "util/String.h"

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

}  // namespace

namespace lsst::qserv::replica {

HttpAsyncReqApp::Ptr HttpAsyncReqApp::create(int argc, char* argv[]) {
    return Ptr(new HttpAsyncReqApp(argc, argv));
}

HttpAsyncReqApp::HttpAsyncReqApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().required("url", "The URL to read data from.", _url)
            .option("method",
                    "The HTTP method. Allowed values: " + util::String::toString(http::allowedMethods),
                    _method, http::allowedMethods)
            .option("header",
                    "The HTTP header to be sent with a request. Note this test application allows"
                    " only one header. The format of the header is '<key>[:<val>]'.",
                    _header);
    parser().option("data", "The data to be sent in the body of a request.", _data);
    parser().option("max-response-data-size",
                    "The maximum size (bytes) of the response body. If a value of the parameter is set"
                    " to 0 then the default limit of 8M imposed by the Boost.Beast library will be assumed.",
                    _maxResponseBodySize);
    parser().option("expiration-ival-sec",
                    "A timeout to wait before the completion of a request. The expiration timeout includes"
                    " all phases of the request's execution, including establishing a connection"
                    " to the server, sending the request and waiting for the server's response."
                    " If a value of the parameter is set to 0 then no expiration timeout will be"
                    " assumed for the request.",
                    _expirationIvalSec);
    parser().option("file",
                    "A path to an output file where the response body received from a remote source will"
                    "  be written. This option is ignored if the flag --body is not specified.",
                    _file);
    parser().flag("result2json",
                  "If specified the flag will cause the application to interpret the response body as"
                  " a JSON object.",
                  _result2json);
    parser().flag("verbose",
                  "The flag that allows printing the completion status and the response header"
                  " info onto the standard output stream.",
                  _verbose);
    parser().flag("body",
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
    auto const method = http::string2method(_method);
    auto const ptr = http::AsyncReq::create(
            io_service, [this, osPtr](auto const& ptr) { this->_dump(ptr, osPtr); }, method, _url, _data,
            headers);
    ptr->setMaxResponseBodySize(_maxResponseBodySize);
    ptr->setExpirationIval(_expirationIvalSec);
    ptr->start();
    io_service.run();

    if (nullptr != osPtr) {
        osPtr->flush();
        if (fs.is_open()) fs.close();
    }
    return ptr->state() == http::AsyncReq::State::FINISHED ? 0 : 1;
}

void HttpAsyncReqApp::_dump(shared_ptr<http::AsyncReq> const& ptr, ostream* osPtr) const {
    if (_verbose) {
        cout << "Request completion state: " << http::AsyncReq::state2str(ptr->state())
             << ", error message: " << ptr->errorMessage() << endl;
    }
    if (ptr->state() == http::AsyncReq::State::FINISHED ||
        ptr->state() == http::AsyncReq::State::BODY_LIMIT_ERROR) {
        if (_verbose) {
            cout << "  HTTP response code: " << ptr->responseCode() << "\n";
            cout << "  response header:\n";
            for (auto&& elem : ptr->responseHeader()) {
                cout << "    " << elem.first << ": " << elem.second << "\n";
            }
        }
        if (ptr->state() == http::AsyncReq::State::FINISHED) {
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

}  // namespace lsst::qserv::replica
