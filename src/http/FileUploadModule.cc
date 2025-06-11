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
#include "http/FileUploadModule.h"

// System headers
#include <memory>

// Third-party headers
#include <httplib.h>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestQuery.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::http {

FileUploadModule::FileUploadModule(string const& authKey, string const& adminAuthKey,
                                   httplib::Request const& req, httplib::Response& resp,
                                   httplib::ContentReader const& contentReader)
        : BaseModule(authKey, adminAuthKey), _req(req), _resp(resp), _contentReader(contentReader) {}

void FileUploadModule::execute(string const& subModuleName, http::AuthType const authType) {
    _subModuleName = subModuleName;
    try {
        if (!_req.is_multipart_form_data()) {
            throw AuthError(context() + "the request is not a multipart form data");
        }
        unique_ptr<httplib::MultipartFormData> currentFile;
        auto const processEndOfEntry = [&]() {
            if (currentFile != nullptr) {
                if (!currentFile->filename.empty()) {
                    onEndOfFile();
                } else {
                    body().objJson[currentFile->name] = currentFile->content;
                }
            }
        };
        _contentReader(
                [&](httplib::MultipartFormData const& file) -> bool {
                    processEndOfEntry();
                    if (!file.filename.empty()) {
                        enforceAuthorization(authType);
                        onStartOfFile(file.name, file.filename, file.content_type);
                    }
                    currentFile.reset(new httplib::MultipartFormData(file));
                    return true;
                },
                [&](char const* data, size_t length) -> bool {
                    if (currentFile->filename.empty()) {
                        currentFile->content.append(data, length);
                    } else {
                        onFileData(data, length);
                    }
                    return true;
                });
        processEndOfEntry();
        json result = onEndOfBody();
        sendData(result);
    } catch (AuthError const& ex) {
        sendError(__func__, "failed to pass authorization requirements, ex: " + string(ex.what()));
    } catch (http::Error const& ex) {
        sendError(ex.func(), ex.what(), ex.errorExt());
    } catch (invalid_argument const& ex) {
        sendError(__func__, "invalid parameters of the request, ex: " + string(ex.what()));
    } catch (exception const& ex) {
        sendError(__func__, "operation failed due to: " + string(ex.what()));
    }
}
string FileUploadModule::method() const { return _req.method; }

unordered_map<string, string> FileUploadModule::params() const { return _req.path_params; }

RequestQuery FileUploadModule::query() const {
    // TODO: The query parameters in CPP-HTTPLIB are stored in the std::multimap
    // container to allow accumulating values of non-unique keys. For now we need
    // to convert the multimap to the std::unordered_map container. This may result
    // in losing some query parameters if they have the same key but different values.
    // Though, the correct solution is to fix the QHTTP library to support
    // the std::multimap container for query parameters.
    unordered_map<string, string> queryParams;
    for (auto const& [key, value] : _req.params) queryParams[key] = value;
    return RequestQuery(queryParams);
}

string FileUploadModule::headerEntry(string const& key) const {
    auto it = _req.headers.find(key);
    return (it != _req.headers.end()) ? it->second : "";
}

void FileUploadModule::sendResponse(string const& content, string const& contentType) {
    _resp.set_content(content, contentType);
}

}  // namespace lsst::qserv::http
