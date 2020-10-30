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
#ifndef LSST_QSERV_HTTPFILEREADER_H
#define LSST_QSERV_HTTPFILEREADER_H

// System headers
#include <functional>
#include <string>
#include <vector>
#include "curl/curl.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpFileReader is a simple interface for pulling files over the HTTP protocol.
 * The implementation of the class breaks an input byte stream into lines and
 * invokes a user-supplied callback (lambda) function for each such line.
 * 
 * The file is required to be ended with the newline character.
 * 
 * Here is an example of using the class to pull a file and dump its content on
 * to the standard output stream:
 * @code
 *   HttpFileReader reader("GET", "http://my.host.domain/data/chunk_0.txt");
 *   reader.read([](std::string const& line) {
 *       std::cout << line << "\n";
 *   });
 * @code
 */
class HttpFileReader {
public:
    /// The function type for notifications on each line parsed in the input stream.
    typedef std::function<void(std::string const&)> CallbackType;

    // No copy semantics for this class.
    HttpFileReader() = delete;
    HttpFileReader(HttpFileReader const&) = delete;
    HttpFileReader& operator=(HttpFileReader const&) = delete;

    /// Non-trivial destructor is needed to free up allocated resources.
    ~HttpFileReader();

    /**
     * @param method  The name of an HTTP method ('GET', 'POST', 'PUT', 'DELETE').
     * @param url A location of a file to be retrieved.
     * @param data Optional data to be sent with a request (depends on the HTTP headers).
     * @param headers Optional HTTP headers to be send with a request.
     */
    HttpFileReader(std::string const& method,
                   std::string const& url,
                   std::string const& data=std::string(),
                   std::vector<std::string> const& headers=std::vector<std::string>());

    /**
     * Begin processing a request. The whole content of a file (or a data source)
     * refferred to by a URL passed into the constructor will be read. The stream will
     * get split into lines. A callback for each such line will be called once
     * the newline character has been encountered. The neline character is not
     * included into a string passed into the callback function.
     *
     * @note This method is safe to be called multiple times.
     * @param onEachLine A pointer to a function to be called on each line read from an
     *   input stream.
     * @throw std::invalid_argument If a non-valid (empty) function pointer was provided.
     * @throw std::runtime_error If the file didn't have the newline character in its end.
     *   The same exception is thrown for any errors encountered during a file retrieval.
     */
    void read(CallbackType const& onEachLine);
 
private:
    /**
     * Check for an error condition.
     *
     * @param scope A location from which the method was called (used for error reporting).
     * @param errnum A result reported by the CURL library function.
     * @throw std::runtime_error If the error-code is not CURL_OK.
     */
    static void _errorChecked(std::string const& scope, CURLcode errnum);

    /**
     * Non-member function declaration used for pushing chunks of data retrieved from
     * an input stream managed by libcurl into the class's method _store().
     * 
     * See the implementation of the class for further details on the function.
     * See the documentation on lincurl C API for an explanation of the function's parameters.
     */
    friend size_t forwardToHttpFileReader(char* ptr, size_t size, size_t nmemb, void* userdata);

    /**
     * This method is invoked by function forwardToHttpFileReader() on each chunk of data
     * reported by CURL while streaming in data from a remote server.
     *
     * @param ptr A pointer to the beginning of the data buffer.
     * @param nchars The number of characters in the buffer.
     */
    void _store(char const* ptr, size_t nchars);

    // Input parameters
    std::string const _method;
    std::string const _url;
    std::string const _data;
    std::vector<std::string> const _headers;

    CallbackType _onEachLine;   ///< set by method read() before pulling a file

    // Cached members
    CURL* _hcurl = nullptr;
    curl_slist* _hlist = nullptr;
    std::string _line;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPFILEREADER_H
