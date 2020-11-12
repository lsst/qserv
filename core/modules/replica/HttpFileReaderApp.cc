

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
#include "replica/HttpFileReaderApp.h"

// System headers
#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/HttpFileReader.h"

using namespace std;

namespace {

string const description =
    "This application reads files from an object store over the HTTP/HTTPS protocol."
    " If option '--file=<file>' is present the file's content will be writted into the"
    " specified file. Otherwise the content will be printed to the standard output stream.";

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

HttpFileReaderApp::Ptr HttpFileReaderApp::create(int argc, char* argv[]) {
    return Ptr(new HttpFileReaderApp(argc, argv));
}


HttpFileReaderApp::HttpFileReaderApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            false   /* injectDatabaseOptions */,
            false   /* boostProtobufVersionCheck */,
            false   /* enableServiceProvider */
        ) {

    parser().required(
        "url",
        "The URL to read data from.",
        _url
    ).option(
        "method",
        "The HTTP method. Allowed values: GET, POST, PUT, DELETE.",
        _method
    ).option(
        "header",
        "The HTTP header to be sent with a request. Note this application allows"
        " only one header.",
        _header
    ).option(
        "data",
        "The data to be sent in the body of a request.",
        _data
    ).reversedFlag(
        "no-ssl-verify-host",
        "The flag that disables verifying the certificate's name against host.",
        _fileReaderConfig.sslVerifyHost
    ).reversedFlag(
        "no-ssl-verify-peer",
        "The flag that disables verifying the peer's SSL certificate.",
        _fileReaderConfig.sslVerifyPeer
    ).option(
        "ca-path",
        "A path to a directory holding CA certificates to verify the peer with."
        " This option is ignored if flag --no-ssl-verify-peer is specified.",
        _fileReaderConfig.caPath
    ).option(
        "ca-info",
        "A path to a Certificate Authority (CA) bundle to verify the peer with."
        " This option is ignored if flag --no-ssl-verify-peer is specified.",
        _fileReaderConfig.caInfo
    ).reversedFlag(
        "no-proxy-ssl-verify-host",
        "The flag that disables verifying the certificate's name against proxy's host.",
        _fileReaderConfig.proxySslVerifyHost
    ).reversedFlag(
        "no-proxy-ssl-verify-peer",
        "The flag that disables verifying the proxy's SSL certificate.",
        _fileReaderConfig.proxySslVerifyPeer
    ).option(
        "proxy-ca-path",
        "A path to a directory holding CA certificates to verify the proxy with."
        " This option is ignored if flag --no-proxy-ssl-verify-peer is specified.",
        _fileReaderConfig.proxyCaPath
    ).option(
        "proxy-ca-info",
        "A path to a Certificate Authority (CA) bundle to verify the proxy with."
        " This option is ignored if flag --no-proxy-ssl-verify-peer is specified.",
        _fileReaderConfig.proxyCaInfo
    ).option(
        "file",
        "A path to an output file where the content received from a remote source will be written."
        "  If the option is not specified then the content will be printed onto the standard output"
        " stream. This option is ignored if flag --silent is specified.",
        _file
    ).flag(
        "silent",
        "The flag that disables printing or writing the content received from a remote"
        " source.",
        _silent
    );
}


int HttpFileReaderApp::runImpl() {
    string const context = "HttpFileReaderApp::" + string(__func__) + "  ";
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
            fs.open(_file, ios::out|ios::trunc);
            if (!fs.is_open()) {
                throw runtime_error(context + "failed to open/create file: " + _file);
            }
            osPtr = &fs;
        }
    }
    HttpFileReader reader(_method, _url, _data, headers, _fileReaderConfig);
    reader.read([&](string const& line) {
        if (nullptr != osPtr) *osPtr << line << "\n";
    });
    if (nullptr != osPtr) {
        osPtr->flush();
        if (fs.is_open()) fs.close();
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
