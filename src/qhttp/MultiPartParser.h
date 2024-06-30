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
#ifndef LSST_QSERV_QHTTP_MULTIPARTPARSER_H
#define LSST_QSERV_QHTTP_MULTIPARTPARSER_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

// Third party headers
#include "boost/noncopyable.hpp"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"

// This header declarations
namespace lsst::qserv::qhttp {

/**
 * ContentHeader class represents a parsed header of a multipart content's entry.
 */
class ContentHeader {
public:
    explicit ContentHeader(std::string const& str);
    ContentHeader() = default;
    ContentHeader(ContentHeader const&) = default;
    ContentHeader& operator=(ContentHeader const&) = default;

    bool valid() const { return !_name.empty(); }
    std::string const str() const { return _str; }
    std::string const& operator[](std::string const& key) const { return _header.at(key); }
    std::string const& name() const { return _name; }
    std::string const& filename() const { return _filename; }
    std::string const& contentType() const { return _contentType; }
    bool isFile() const { return !_filename.empty(); }

private:
    std::string _trim(std::string const& str) const;
    void _parseHeader();
    void _parseContentDisposition();
    void _parseContentType();

    std::string const _str;
    std::unordered_map<std::string, std::string> _header;
    std::string _name;
    std::string _filename;
    std::string _contentType;
};

/**
 * RequestProcessor class is an abstract base class for processing multipart content entries
 * regardless of an implementation of the parser. The class provides a set of virtual methods
 * to be derived and implemebted by a subclass.
 *
 * IMPORTANT: to terminate the ongoing parsing the user should return 'false' from
 * the coresponding methods.
 */
class RequestProcessor : boost::noncopyable {
public:
    explicit RequestProcessor(qhttp::Response::Ptr response_) : response(response_) {}
    virtual ~RequestProcessor() = default;

    virtual bool onParamValue(ContentHeader const& hdr, std::string const& name,
                              std::string_view const& value) = 0;
    virtual bool onFileOpen(ContentHeader const& hdr, std::string const& name, std::string const& filename,
                            std::string const& contentType) = 0;
    virtual bool onFileContent(std::string_view const& data) = 0;
    virtual bool onFileClose() = 0;
    virtual void onFinished(std::string const& error) = 0;

protected:
    qhttp::Response::Ptr const response;
};

/**
 * MultiPartParser class is a simple parser for multipart content in HTTP requests.
 * The class is designed to read the whole content of a request and parse it into separate
 * entries. Each entry consists of a header and a content. The implementation guarantees
 * that the content is stored in memory. The class would work with request handlers
 * of both types specified via the parameter 'readEntireBody' of the type HandlerSpec
 * (see the class Server for more details).
 *
 * IMPORTANT: The class is not designed for parsing large files. It is recommended to use
 * the class for parsing small files which fit into memory. Also, it's a responsibility of
 * the user to sent a proper response back to a caller when the parsing completion event
 * is triggered (a user-supplied callback of the type MultiPartParser::onFinish is called
 * by the parser). The parser will automatically detect the content type of a request
 * to ensure it's "multipart/form-data". If it's not, the parser will report an error
 * to a caller via the above-mentioned event channel (MultiPartParser::onFinish).
 * This callback is called with a non-empty value if 'error' to indicate a failure in this case.
 *
 * The are two ways to use the MultiPartParser class. The first way is to configure it
 * with a set of callbacks for each type of the parser's events to be conveyed to a user
 * for entries found in the request. In this case it's a responsibility of the user
 * to record a state of the parsing process. The following example demonstrates how to use
 * the MultiPartParser class with a set of callbacks.
 * @code
 *   struct Context {
 *       // The user-defined context and state of the parsing.
 *       qhttp::Response::Ptr response;
 *       ...
 *   };
 *
 *   httpServer->addHandler(
 *       "POST",
 *       "/svc",
 *       [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
 *           auto context = std::make_shared<Context>({resp, ... });
 *           qhttp::MultiPartParser::parse(
 *               req,
 *
 *               // -- onParamValue --
 *               [context](auto const& hdr,
 *                         auto const& name,
 *                         auto const& value) -> bool {
 *                   context-> ... ;
 *                   return true;
 *               },
 *
 *               // -- onFileOpen --
 *               [context](auto const& hdr,
 *                         auto const& name,
 *                         auto const& filename,
 *                         auto const& contentType) -> bool {
 *                   context-> ... ;
 *                   return true;
 *               },
 *
 *               // -- onFileContent --
 *               [context](auto const& data) -> bool {
 *                   context-> ... ;
 *                   return true;
 *               },
 *
 *               // -- onFileClose --
 *               [context]() -> bool {
 *                   context-> ... ;
 *                   return true;
 *               },
 *
 *               // -- onFinished --
 *               [context](auto const& error) {
 *                   context-> ... ;
 *                   resp->sendStatus(qhttp::STATUS_OK);
 *               }
 *           );
 *       }
 *   );
 * @endcode
 * A better way to use the MultiPartParser is to pass it an instance of a processor class
 * that derives from the abstract base class RequestProcessor class.
 * @code
 *   class DummyRequestProcessor : public RequestProcessor {
 *   public:
 *       explicit DummyRequestProcessor(qhttp::Response::Ptr response) : RequestProcessor(response) {}
 *
 *       virtual bool onParamValue(qhttp::ContentHeader const& hdr,
 *                                 std::string const& name,
 *                                 std::string_view const& value) {
 *           return true;
 *       }
 *       virtual bool onFileOpen(qhttp::ContentHeader const& hdr,
 *                               std::string const& name,
 *                               std::string const& filename,
 *                               std::string const& contentType) {
 *           return true;
 *       }
 *       virtual bool onFileContent(std::string_view const& data) {
 *           return true;
 *       }
 *       virtual bool onFileClose() {
 *           return true;
 *       }
 *       virtual void onFinish(std::string const& error) {
 *           if (error.empty()) {
 *                response->sendStatus(qhttp::STATUS_OK);
 *           } else {
 *                std::cerr << __func__ << " Error: " << error << std::endl;
 *                response->sendStatus(qhttp::STATUS_INTERNAL_SERVER_ERR);
 *           }
 *       }
 *   private:
 *       qhttp::Response::Ptr const _response;
 *   };
 *
 *   httpServer->addHandler(
 *       "POST",
 *       "/svc",
 *       [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
 *           qhttp::MultiPartParser::parse(req, std::make_shared<DummyRequestProcessor>(resp));
 *       }
 *   );
 * @endcode
 */
class MultiPartParser : public std::enable_shared_from_this<MultiPartParser>, boost::noncopyable {
public:
    // The callback types for the parser are defined below.
    //
    // OnParamValue(hdr, name, value):
    //     for each parameter found in the content entry
    //
    // OnFileOpen(hdr, name, filename, contentType):
    //     when a file is found in the content entry and before the file content is read
    //     and delivered to a user
    //
    // OnFileContent(data):
    //     for each chunk of the file content
    //
    // OnFileClose():
    //     when a content of the previously open file is read and delivered to a user
    //
    // OnFinished(error):
    //     when the parsing is finished or failed (if the error message is not empty)
    //
    // Note that all (but OnFinished) callbacks return a flag indicating whether the parsing should
    // continue or not. If the flag is set to 'false', the parser will stop the parsing. If the client
    // wants to stop the parsing the parser won't call the OnFinished callback or any other callbacks.

    using OnParamValue =
            std::function<bool(ContentHeader const&, std::string const&, std::string_view const&)>;
    using OnFileOpen = std::function<bool(ContentHeader const&, std::string const&, std::string const&,
                                          std::string const&)>;
    using OnFileContent = std::function<bool(std::string_view const&)>;
    using OnFileClose = std::function<bool()>;
    using OnFinished = std::function<void(std::string const&)>;

    static void parse(qhttp::Request::Ptr request, OnParamValue onParamValue, OnFileOpen onFileOpen,
                      OnFileContent onFileContent, OnFileClose onFileClose, OnFinished onFinished);

    static void parse(qhttp::Request::Ptr request, std::shared_ptr<RequestProcessor> processor);

private:
    MultiPartParser(qhttp::Request::Ptr request, OnParamValue onParamValue, OnFileOpen onFileOpen,
                    OnFileContent onFileContent, OnFileClose onFileClose, OnFinished onFinished);

    MultiPartParser(qhttp::Request::Ptr request, std::shared_ptr<RequestProcessor> processor);

    void _parse();
    void _findBoundary();
    void _readData();
    void _finishedReadData(bool success);
    void _parseBody();

    qhttp::Request::Ptr const _request;
    OnParamValue const _onParamValue;
    OnFileOpen const _onFileOpen;
    OnFileContent const _onFileContent;
    OnFileClose const _onFileClose;
    OnFinished const _onFinished;

    std::string _boundary;
    std::string _content;

    struct ContentEntry {
        ContentHeader header;
        std::string_view content;
    };
    std::list<ContentEntry> _contentEntries;
};

}  // namespace lsst::qserv::qhttp

#endif /* LSST_QSERV_QHTTP_MULTIPARTPARSER_H */
