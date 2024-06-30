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
#include "qhttp/MultiPartParser.h"

// System headers
#include <sstream>

// Third party headers
#include "boost/noncopyable.hpp"

using namespace std;

namespace {

template <typename POINTER_TYPE>
void assertNotNull(POINTER_TYPE ptr, std::string const& message) {
    if (ptr == nullptr) throw std::runtime_error(message);
}

}  // namespace

namespace lsst::qserv::qhttp {

ContentHeader::ContentHeader(std::string const& str) : _str(str) {
    _parseHeader();
    _parseContentDisposition();
    _parseContentType();
}

std::string ContentHeader::_trim(std::string const& str) const { return boost::algorithm::trim_copy(str); }

void ContentHeader::_parseHeader() {
    std::istringstream iss(_str);
    std::string line;
    while (std::getline(iss, line)) {
        size_t const pos = line.find(":");
        if (pos == std::string::npos) continue;
        std::string const key = _trim(line.substr(0, pos));
        std::string const value = _trim(line.substr(pos + 1));
        _header[key] = value;
    }
}

void ContentHeader::_parseContentDisposition() {
    auto const it = _header.find("Content-Disposition");
    if (it == _header.end()) return;
    std::string const& contentDisposition = it->second;
    std::string const nameTag = "name=\"";
    size_t pos = contentDisposition.find(nameTag);
    if (pos != std::string::npos) {
        size_t const start = pos + nameTag.size();
        size_t const end = contentDisposition.find("\"", start);
        _name = _trim(contentDisposition.substr(start, end - start));
    }
    std::string const filenameTag = "filename=\"";
    pos = contentDisposition.find(filenameTag);
    if (pos != std::string::npos) {
        size_t const start = pos + filenameTag.size();
        size_t const end = contentDisposition.find("\"", start);
        _filename = _trim(contentDisposition.substr(start, end - start));
    }
}

void ContentHeader::_parseContentType() {
    auto const it = _header.find("Content-Type");
    if (it == _header.end()) return;
    _contentType = _trim(it->second);
}

void MultiPartParser::parse(qhttp::Request::Ptr request, OnParamValue onParamValue, OnFileOpen onFileOpen,
                            OnFileContent onFileContent, OnFileClose onFileClose, OnFinished onFinished) {
    ::assertNotNull(request, "request is nullptr");
    ::assertNotNull(onParamValue, "onParamValue callback is nullptr");
    ::assertNotNull(onFileOpen, "onFileOpen callback is nullptr");
    ::assertNotNull(onFileContent, "onFileContent callback is nullptr");
    ::assertNotNull(onFileClose, "onFileClose callback is nullptr");
    ::assertNotNull(onFinished, "onFinished callback is nullptr");
    std::shared_ptr<MultiPartParser>(
            new MultiPartParser(request, onParamValue, onFileOpen, onFileContent, onFileClose, onFinished))
            ->_parse();
}

void MultiPartParser::parse(qhttp::Request::Ptr request, std::shared_ptr<RequestProcessor> processor) {
    ::assertNotNull(request, "request is nullptr");
    ::assertNotNull(processor, "processor is nullptr");
    std::shared_ptr<MultiPartParser>(new MultiPartParser(request, processor))->_parse();
}

MultiPartParser::MultiPartParser(qhttp::Request::Ptr request, OnParamValue onParamValue,
                                 OnFileOpen onFileOpen, OnFileContent onFileContent, OnFileClose onFileClose,
                                 OnFinished onFinished)
        : _request(request),
          _onParamValue(onParamValue),
          _onFileOpen(onFileOpen),
          _onFileContent(onFileContent),
          _onFileClose(onFileClose),
          _onFinished(onFinished) {}

MultiPartParser::MultiPartParser(qhttp::Request::Ptr request, std::shared_ptr<RequestProcessor> processor)
        : _request(request),
          _onParamValue([processor](auto const& hdr, auto const& name, auto const& value) {
              return processor->onParamValue(hdr, name, value);
          }),
          _onFileOpen([processor](auto const& hdr, auto const& name, auto const& filename,
                                  auto const& contentType) {
              return processor->onFileOpen(hdr, name, filename, contentType);
          }),
          _onFileContent([processor](auto const& data) { return processor->onFileContent(data); }),
          _onFileClose([processor]() { return processor->onFileClose(); }),
          _onFinished([processor](auto const& error) { processor->onFinished(error); }) {}

void MultiPartParser::_parse() {
    try {
        _findBoundary();
        _readData();
    } catch (runtime_error const& ex) {
        _onFinished(ex.what());
    }
}

void MultiPartParser::_findBoundary() {
    std::string const& contentType = _request->header.at("Content-Type");
    std::string const multiFormPattern = "multipart/form-data; boundary=";
    if (auto const pos = contentType.find(multiFormPattern); pos != std::string::npos) {
        _boundary = contentType.substr(pos + multiFormPattern.size());
    } else {
        throw runtime_error("Content-Type is not multipart/form-data");
    }
}

void MultiPartParser::_readData() {
    _request->readPartialBodyAsync(
            [self = shared_from_this()](auto request, auto response, bool success, size_t numBytes) {
                self->_finishedReadData(success);
            });
}

void MultiPartParser::_finishedReadData(bool success) {
    if (!success) {
        _onFinished("failed to read the request body");
        return;
    }
    _content.append(istreambuf_iterator<char>(_request->content), {});
    bool const doneReading = _request->contentReadBytes() == _request->contentLengthBytes();
    if (doneReading) {
        _parseBody();
    } else {
        _readData();
    }
}

void MultiPartParser::_parseBody() {
    std::string const delimiter = "--" + _boundary;
    std::string const endDelimiter = delimiter + "--";

    for (size_t pos = 0; pos != std::string::npos;) {
        size_t const start = _content.find(delimiter, pos);
        if (start == std::string::npos) break;

        size_t const end = _content.find("\r\n\r\n", start);
        if (end == std::string::npos) break;

        size_t headerStart = start + delimiter.size() + 2;
        size_t headerEnd = end;
        auto header = ContentHeader(_content.substr(headerStart, headerEnd - headerStart));
        if (!header.valid()) {
            _onFinished("invalid content header found in the request body");
            return;
        }
        size_t contentStart = end + 4;
        size_t contentEnd = _content.find(delimiter, contentStart);
        if (contentEnd == std::string::npos) {
            contentEnd = _content.find(endDelimiter, contentStart);
        }
        if (contentEnd == std::string::npos) {
            _onFinished("failed to find the end of the content entry in the request body");
            return;
        }
        size_t const contentLength = contentEnd - contentStart - 2;
        _contentEntries.emplace_back(header, std::string_view(&_content[contentStart], contentLength));

        pos = contentEnd;
    }
    for (auto const& entry : _contentEntries) {
        auto const& header = entry.header;
        if (header.isFile()) {
            if (!_onFileOpen(header, header.name(), header.filename(), header.contentType())) return;
            if (!_onFileContent(entry.content)) return;
            if (!_onFileClose()) return;
        } else {
            if (!_onParamValue(header, header.name(), entry.content)) return;
        }
    }
    _onFinished(std::string());
}

}  // namespace lsst::qserv::qhttp
