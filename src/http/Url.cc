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
#include "http/Url.h"

// System headers
#include <map>
#include <stdexcept>

// Qserv headers
#include "global/stringUtil.h"

using namespace std;

namespace lsst::qserv::http {

Url::Url(string const& url) : _url(url) { _translate(); }

string const& Url::fileHost() const {
    if ((_scheme == DATA_JSON) || (_scheme == DATA_CSV) || (_scheme == FILE)) return _fileHost;
    throw logic_error(_error(__func__, "not a file resource."));
}

string const& Url::filePath() const {
    if ((_scheme == DATA_JSON) || (_scheme == DATA_CSV) || (_scheme == FILE)) return _filePath;
    throw logic_error(_error(__func__, "not a file resource."));
}

string const& Url::host() const {
    if ((_scheme == HTTP) || (_scheme == HTTPS)) return _host;
    throw logic_error(_error(__func__, "not an HTTP/HTTPS resource."));
}

uint16_t Url::port() const {
    if ((_scheme == HTTP) || (_scheme == HTTPS)) return _port;
    throw logic_error(_error(__func__, "not an HTTP/HTTPS resource."));
}

string const& Url::target() const {
    if ((_scheme == HTTP) || (_scheme == HTTPS)) return _target;
    throw logic_error(_error(__func__, "not an HTTP/HTTPS resource."));
}

string Url::_error(string const& func, string const& msg) { return "Url::" + func + ": " + msg; }

void Url::_translate() {
    if (_url.empty()) throw invalid_argument(_error(__func__, "url is empty."));

    static map<string, Scheme> const schemes = {{"data-json://", Scheme::DATA_JSON},
                                                {"data-csv://", Scheme::DATA_CSV},
                                                {"file://", Scheme::FILE},
                                                {"http://", Scheme::HTTP},
                                                {"https://", Scheme::HTTPS}};
    for (auto&& itr : schemes) {
        string const& prefix = itr.first;
        Scheme const scheme = itr.second;
        if ((_url.length() > prefix.length()) && (_url.substr(0, prefix.length()) == prefix)) {
            if (Scheme::DATA_JSON == scheme) {
                // This scheme assumes the following format: "data-json://<host>/"
                string const hostFilePath = _url.substr(prefix.length());
                string::size_type const pos = hostFilePath.find_first_of('/');
                if (pos != string::npos) {
                    if ((pos != 0) and (hostFilePath.length() == pos + 1)) {
                        _scheme = scheme;
                        _fileHost = hostFilePath.substr(0, pos);
                        return;
                    }
                }
            } else if (Scheme::DATA_CSV == scheme) {
                // This scheme assumes the following format: "data-csv://<host>/[<file>]"
                string const hostFilePath = _url.substr(prefix.length());
                string::size_type const pos = hostFilePath.find_first_of('/');
                if (pos != string::npos) {
                    if (pos == 0) {
                        // This URL doesn't have the host name: data-csv:///<path>
                        if (hostFilePath.length() > 1) {
                            _scheme = scheme;
                            _filePath = hostFilePath;
                            return;
                        }
                    } else {
                        // This URL has the host name: file://<host>/<path>
                        if (hostFilePath.length() > pos + 1) {
                            _scheme = scheme;
                            _fileHost = hostFilePath.substr(0, pos);
                            _filePath = hostFilePath.substr(pos);
                            return;
                        }
                    }
                }
            } else if (Scheme::FILE == scheme) {
                // Note that the file path should be always absolute in the URL. It's impossible to
                // pass a relative location of a file in this scheme. The file path is required to
                // have at least one character besides the root folder.
                // See details: https://en.wikipedia.org/wiki/File_URI_scheme
                string const hostFilePath = _url.substr(prefix.length());
                string::size_type const pos = hostFilePath.find_first_of('/');
                if (pos != string::npos) {
                    if (pos == 0) {
                        // This URL doesn't have the host name: file:///<path>
                        if (hostFilePath.length() > 1) {
                            _scheme = scheme;
                            _filePath = hostFilePath;
                            return;
                        }
                    } else {
                        // This URL has the host name: file://<host>/<path>
                        if (hostFilePath.length() > pos + 1) {
                            _scheme = scheme;
                            _fileHost = hostFilePath.substr(0, pos);
                            _filePath = hostFilePath.substr(pos);
                            return;
                        }
                    }
                }
            } else {
                // Non-empty host is the only component that's required by the algorithm.
                string const hostPortTarget = _url.substr(prefix.length());
                if (!hostPortTarget.empty()) {
                    string target;
                    string::size_type const posTarget = hostPortTarget.find_first_of('/');
                    if (posTarget != string::npos) {
                        target = hostPortTarget.substr(posTarget);
                    }
                    uint16_t port = 0;
                    string const hostPort = hostPortTarget.substr(0, posTarget);
                    string::size_type const posPort = hostPort.find_first_of(':');
                    if (posPort != string::npos) {
                        port = lsst::qserv::stoui(hostPort.substr(posPort + 1));
                    }
                    string const host = hostPort.substr(0, posPort);
                    if (!host.empty()) {
                        _scheme = scheme;
                        _host = host;
                        _port = port;
                        _target = target;
                        return;
                    }
                }
            }
        }
    }
    throw invalid_argument(_error(__func__, "invalid url '" + _url + "'"));
}

}  // namespace lsst::qserv::http
