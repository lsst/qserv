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
#include "replica/HttpFileReader.h"

// System headers
#include <stdexcept>

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

size_t forwardToHttpFileReader(char* ptr, size_t size, size_t nmemb, void* userdata) {
    size_t const nchars = size * nmemb;
    HttpFileReader* reader = reinterpret_cast<HttpFileReader*>(userdata);
    reader->_store(ptr, nchars);
    return nchars;
}


HttpFileReader::HttpFileReader(string const& method,
                               string const& url,
                               string const& data,
                               vector<string> const& headers)
    :   _method(method),
        _url(url),
        _data(data),
        _headers(headers) {
    _hcurl = curl_easy_init();
    if (_hcurl == nullptr) throw runtime_error("curl_easy_init() failed");
}


HttpFileReader::~HttpFileReader() {
    curl_slist_free_all(_hlist);
    curl_easy_cleanup(_hcurl);
}


void HttpFileReader::read(CallbackType const& onEachLine) {
    string const context = "HttpFileReader::" + string(__func__) + " ";
    if (onEachLine == nullptr) throw invalid_argument(context + "illegal callback function.");
    _onEachLine = onEachLine;
    _errorChecked("curl_easy_setopt(CURLOPT_URL)", curl_easy_setopt(_hcurl, CURLOPT_URL, _url.c_str()));
    _errorChecked("curl_easy_setopt(CURLOPT_CUSTOMREQUEST)", curl_easy_setopt(_hcurl, CURLOPT_CUSTOMREQUEST, nullptr));
    if (_method == "GET") {
        _errorChecked("curl_easy_setopt(CURLOPT_HTTPGET)", curl_easy_setopt(_hcurl, CURLOPT_HTTPGET, 1L));
    } else if (_method == "POST") {
        _errorChecked("curl_easy_setopt(CURLOPT_POST)", curl_easy_setopt(_hcurl, CURLOPT_POST, 1L));
    } else {
        _errorChecked("curl_easy_setopt(CURLOPT_CUSTOMREQUEST)", curl_easy_setopt(_hcurl, CURLOPT_CUSTOMREQUEST, _method.c_str()));
    }
    if (!_data.empty()) {
        _errorChecked("curl_easy_setopt(CURLOPT_POSTFIELDS)", curl_easy_setopt(_hcurl, CURLOPT_POSTFIELDS, _data.c_str()));
        _errorChecked("curl_easy_setopt(CURLOPT_POSTFIELDSIZE)", curl_easy_setopt(_hcurl, CURLOPT_POSTFIELDSIZE, _data.size()));
    }
    curl_slist_free_all(_hlist);
    _hlist = nullptr;
    for(auto& header: _headers) {
        _hlist = curl_slist_append(_hlist, header.c_str());
    }
    _errorChecked("curl_easy_setopt(CURLOPT_HTTPHEADER)", curl_easy_setopt(_hcurl, CURLOPT_HTTPHEADER, _hlist));
    _errorChecked("curl_easy_setopt(CURLOPT_WRITEFUNCTION)", curl_easy_setopt(_hcurl, CURLOPT_WRITEFUNCTION, forwardToHttpFileReader));
    _errorChecked("curl_easy_setopt(CURLOPT_WRITEDATA)", curl_easy_setopt(_hcurl, CURLOPT_WRITEDATA, this));
    _line.erase();
    _errorChecked("curl_easy_perform()", curl_easy_perform(_hcurl));
    if (!_line.empty()) throw runtime_error(context + "no newline in the end of the input stream");
}


void HttpFileReader::_errorChecked(string const& scope, CURLcode errnum) {
    if (errnum != CURLE_OK) {
        throw runtime_error(scope + " failed with: " + curl_easy_strerror(errnum));
    }
}


void HttpFileReader::_store(char const* ptr, size_t nchars) {
    char const* begin = ptr;
    char const* end = ptr + nchars;
    char const* current = begin;
    while (current < end) {
        char const c = *(current++);
        if (c == '\n') {
            // Don't push the newline character into the line.
            _line.append(begin, current - begin - 1);
            _onEachLine(_line);
            _line.erase();
            begin = current;
        }
    }
    // Store the begining of the next string (if any) which hasn't been terminated yet
    // with the newline character. This string will get completed when the next chunk of
    // the input data will arrive.
    if (current != begin) _line.append(begin, current - begin);
}

}}} // namespace lsst::qserv::replica
