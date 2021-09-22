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
#include "replica/Url.h"

// System headers
#include <stdexcept>

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

Url::Url(string const& url)
    :   _url(url) {
    _translate();
}


string const& Url::fileHost() const {
    if (_scheme == FILE) return _fileHost;
    throw logic_error(_error(__func__, "not a file resource."));
}


string const& Url::filePath() const {
    if (_scheme == FILE) return _filePath;
    throw logic_error(_error(__func__, "not a file resource."));
}


string Url::_error(string const& func, string const& msg) {
    return "Url::" + func + ": " + msg;
}


void Url::_translate() {
    if (_url.empty()) throw invalid_argument(_error(__func__, "url is empty."));

    // Note that the file path should be always absolute in the URL. It's impossible to
    // pass a relative location of a file in this scheme. The file path is required to
    // have at least one character besides the root folder.
    // See details: https://en.wikipedia.org/wiki/File_URI_scheme
    string scheme = "file://";
    if ((_url.length() > scheme.length()) && (_url.substr(0, scheme.length()) == scheme)) {
        string const hostFilePath = _url.substr(scheme.length());
        string::size_type const pos = hostFilePath.find_first_of('/');
        if (pos != string::npos) {
            if (pos == 0) {
                // This URL doesn't have the host name: file:///<path>
                if (hostFilePath.length() > 1) {
                    _scheme = FILE;
                    _filePath = hostFilePath;
                    return;
                }
            } else {
                // This URL has the host name: file://<host>/<path>
                if (hostFilePath.length() > pos + 1) {
                    _scheme = FILE;
                    _fileHost = hostFilePath.substr(0, pos);
                    _filePath = hostFilePath.substr(pos);
                    return;
                }
            }
        }
    }
    scheme = "http://";
    if ((_url.length() > scheme.length()) && (_url.substr(0, scheme.length()) == scheme)) {
        _scheme = HTTP;
        return;
    }
    scheme = "https://";
    if ((_url.length() > scheme.length()) && (_url.substr(0, scheme.length()) == scheme)) {
        _scheme = HTTPS;
        return;
    }
    throw invalid_argument(_error(__func__, "invalid url '" + _url + "'"));
}

}}} // namespace lsst::qserv::replica
