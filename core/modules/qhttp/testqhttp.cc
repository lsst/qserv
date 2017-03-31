/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#define BOOST_TEST_MODULE qhttp
#include "boost/test/included/unit_test.hpp"

#include <chrono>
#include <fstream>
#include <sstream>
#include <set>
#include <string>
#include <thread>

#include "boost/asio.hpp"
#include "boost/algorithm/string/join.hpp"
#include "boost/range/adaptors.hpp"
#include "curl/curl.h"

#include "qhttp/Server.h"

namespace asio = boost::asio;
namespace ip = boost::asio::ip;

namespace {


void compareWithFile(std::string const& content, std::string const& file)
{
    std::ifstream f(file);
    BOOST_TEST(f.good());
    std::stringstream s;
    s << f.rdbuf();
    BOOST_TEST(s.str() == content);
}


std::string printParams(lsst::qserv::qhttp::Request::Ptr const& req)
{
    std::map<std::string, std::string> pparams;
    for(auto const& pparam: req->params) {
        pparams[pparam.first] = pparam.first + "=" + pparam.second;
    }
    std::map<std::string, std::string> pquerys;
    for(auto const& pquery: req->query) {
        pquerys[pquery.first] = pquery.first + "=" + pquery.second;
    }
    return std::string("params[") + boost::join(pparams | boost::adaptors::map_values, ",") + "] "
        + "query[" + boost::join(pquerys | boost::adaptors::map_values, ",") + "]";
}


size_t writeToString(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    size_t nchars = size * nmemb;
    std::string* str = reinterpret_cast<std::string*>(userdata);
    str->append(ptr, nchars);
    return nchars;
}


//
//----- CurlEasy is a helper class for issuing HTTP requests and validating responses using the
//      libcurl "easy" API.  Works with CurlMulti class below.  See http://curl.haxx.se/libcurl/c/
//      for API details.
//

class CurlEasy
{
public:

    CurlEasy();
    ~CurlEasy();

    CurlEasy& setup(
        std::string const& method,
        std::string const& url,
        std::string const& data
    );

    CurlEasy& perform();

    CurlEasy& validate(
        int responseCode,
        std::string const& contentType
    );

    CURL* hcurl;
    std::string recdContent;
};


CurlEasy::CurlEasy()
{
    hcurl = curl_easy_init();
    BOOST_TEST(hcurl != static_cast<CURL *>(nullptr));
}


CurlEasy::~CurlEasy()
{
    curl_easy_cleanup(hcurl);
}


CurlEasy& CurlEasy::setup(
    std::string const& method,
    std::string const& url,
    std::string const& data)
{
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_URL, url.c_str()) == CURLE_OK);

    if (method == "GET") {
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_CUSTOMREQUEST, nullptr) == CURLE_OK);
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_HTTPGET, 1L) == CURLE_OK);
    } else if (method == "POST") {
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_CUSTOMREQUEST, nullptr) == CURLE_OK);
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_POST, 1L) == CURLE_OK);
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_POSTFIELDS, data.c_str()) == CURLE_OK);
    } else {
        BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_CUSTOMREQUEST, method.c_str()) == CURLE_OK);
    }

    recdContent.erase();
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_WRITEFUNCTION, writeToString) == CURLE_OK);
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_WRITEDATA, &recdContent) == CURLE_OK);

    return *this;
}


CurlEasy& CurlEasy::perform()
{
    BOOST_TEST(curl_easy_perform(hcurl) == CURLE_OK);
    return *this;
}


CurlEasy& CurlEasy::validate(
    int responseCode,
    std::string const& contentType)
{
    long recdResponseCode;
    char *recdContentType = nullptr;
    double recdContentLength;

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_RESPONSE_CODE, &recdResponseCode) == CURLE_OK);
    BOOST_TEST(recdResponseCode == responseCode);

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_CONTENT_TYPE, &recdContentType) == CURLE_OK);
    BOOST_TEST(recdContentType == contentType);

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &recdContentLength) == CURLE_OK);
    BOOST_TEST(recdContentLength == recdContent.size());

    return *this;
}


//
//----- CurlMutli is a helper class for managing multiple concurrent HTTP requests within a
//      single thread, using the libcurl "Multi" API.  Works with the "CurlEasy" class above.
//      http://curl.haxx.se/libcurl/c/libcurl-multi.html for API details.
//

class CurlMulti
{
public:

    CurlMulti();
    ~CurlMulti();

    void add(CurlEasy& c, std::function<void()> const& handler);
    void perform(int msecs);

    CURLM* hcurlm;
    std::map<CURL*, std::function<void()>> handlers;
};


CurlMulti::CurlMulti()
{
    hcurlm = curl_multi_init();
    BOOST_TEST(hcurlm != static_cast<CURLM *>(nullptr));
}


CurlMulti::~CurlMulti()
{
    curl_multi_cleanup(hcurlm);
}


void CurlMulti::add(CurlEasy& c, std::function<void()> const& handler)
{
    handlers[c.hcurl] = handler;
    BOOST_TEST(curl_multi_add_handle(hcurlm, c.hcurl) == CURLM_OK);
}


void CurlMulti::perform(int msecs)
{
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(msecs);
    while(std::chrono::steady_clock::now() < end) {

        int runningHandles = 0;
        BOOST_TEST(curl_multi_perform(hcurlm, &runningHandles) == CURLM_OK);

        int msgsInQueue;
        CURLMsg *msg;
        while((msg = curl_multi_info_read(hcurlm, &msgsInQueue)) != nullptr) {
            BOOST_TEST(curl_multi_remove_handle(hcurlm, msg->easy_handle) == CURLM_OK);
            auto hit = handlers.find(msg->easy_handle);
            if (hit != handlers.end()) hit->second();
        }

        if (runningHandles == 0) return;

        int numfds = 0;
        auto remaining = end - std::chrono::steady_clock::now();
        msecs = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
        BOOST_TEST(curl_multi_wait(hcurlm, nullptr, 0, msecs, &numfds) == CURLM_OK);
    }
}

} // anon. namespace

namespace lsst {
namespace qserv {

//
//----- The test fixture instantiates a qhttp server and a boost::asio::io_service to run it,
//      manages a thread that runs the io_service, and handles global init and cleanup of libcurl.

struct QhttpFixture
{
    QhttpFixture()
    {
        server = qhttp::Server::create(service, 0);
        urlPrefix = "http://localhost:" + std::to_string(server->getPort()) + "/";
        BOOST_TEST(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK);
    }

    void start()
    {
        server->accept();
        serviceThread = std::move(std::thread([this](){ service.run(); }));
    }

    ~QhttpFixture()
    {
        service.stop();
        serviceThread.join();
        curl_global_cleanup();
    }

    //
    //----- The only tests below for which we can't use libcurl are the relative link tests, because libcurl
    //      snaps out dot pathname components on the client side.  This alternative sends a GET request and
    //      checks the reply using synchronous asio and regexps directly.  Prefer using libcurl in all other
    //      tests, though, as libcurl is more capable and is externally tested/validated.
    //

    std::string asioHttpGet(
        std::string const& path,
        int responseCode,
        std::string const& contentType)
    {
        boost::system::error_code ec;

        ip::tcp::endpoint endpoint(ip::address::from_string("127.0.0.1"), server->getPort());
        ip::tcp::socket socket(service);
        socket.connect(endpoint, ec);
        BOOST_TEST(!ec);

        std::string req = std::string("GET ") + path + " HTTP/1.1\r\n\r\n";
        asio::write(socket, asio::buffer(req), ec);
        BOOST_TEST(!ec);

        asio::streambuf respbuf;
        std::istream resp(&respbuf);
        size_t bytesRead = asio::read_until(socket, respbuf, "\r\n\r\n", ec);
        BOOST_TEST(!ec);
        size_t bytesBuffered = respbuf.size() - bytesRead;

        std::string line;
        std::map<std::string, std::string> header;
        static boost::regex respRe{"^[^ \\r]+ ([0-9]+) .*\\r$"}; // e.g. "HTTP/1.1 200 OK"
        boost::smatch respMatch;
        if (getline(resp, line) && boost::regex_match(line, respMatch, respRe)) {
            BOOST_TEST(stoi(respMatch[1].str()) == responseCode);
            static boost::regex headerRe{"^([^:\\r]+): ?([^\\r]*)\\r$"}; // e.g. "header: value"
            boost::smatch headerMatch;
            while(getline(resp, line) && boost::regex_match(line, headerMatch, headerRe)) {
                header[headerMatch[1]] = headerMatch[2];
            }
        }

        BOOST_TEST(header["Content-Type"] == contentType);

        if (header.count("Content-Length") > 0) {
            size_t bytesRemaining = stoull(header["Content-Length"]) - bytesBuffered;
            asio::read(socket, respbuf, asio::transfer_exactly(bytesRemaining), ec);
            BOOST_TEST(!ec);
        }

        auto respbegin = asio::buffers_begin(respbuf.data());
        return std::string(respbegin, respbegin + respbuf.size());
    }

    asio::io_service service;
    std::thread serviceThread;
    qhttp::Server::Ptr server;
    std::string urlPrefix;
};


BOOST_FIXTURE_TEST_CASE(static_content, QhttpFixture)
{
    server->addStaticContent("/*", "core/modules/qhttp/testdata");
    start();

    CurlEasy curl;

    //----- test default index.htm

    curl.setup("GET", urlPrefix, "").perform().validate(200, "text/html");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/index.htm");

    //----- test subdirectories and file typing by extension

    curl.setup("GET", urlPrefix + "css/style.css", "").perform().validate(200, "text/css");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/css/style.css");
    curl.setup("GET", urlPrefix + "images/lsst.gif", "").perform().validate(200, "image/gif");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/images/lsst.gif");
    curl.setup("GET", urlPrefix + "images/lsst.jpg", "").perform().validate(200, "image/jpeg");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/images/lsst.jpg");
    curl.setup("GET", urlPrefix + "images/lsst.png", "").perform().validate(200, "image/png");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/images/lsst.png");
    curl.setup("GET", urlPrefix + "js/main.js", "").perform().validate(200, "application/javascript");
    compareWithFile(curl.recdContent, "core/modules/qhttp/testdata/js/main.js");

    //----- test redirect for directory w/o trailing "/"

    char *redirect = nullptr;
    curl.setup("GET", urlPrefix + "css", "").perform().validate(301, "text/html");
    BOOST_TEST(curl.recdContent.find("301") != std::string::npos);
    BOOST_TEST(curl_easy_getinfo(curl.hcurl, CURLINFO_REDIRECT_URL, &redirect) == CURLE_OK);
    BOOST_TEST(redirect == urlPrefix + "css/");

    //----- test non-existent file

    curl.setup("GET", urlPrefix + "doesNotExist", "").perform().validate(404, "text/html");
    BOOST_TEST(curl.recdContent.find("404") != std::string::npos);
}


BOOST_FIXTURE_TEST_CASE(relative_url_containment, QhttpFixture)
{
    server->addStaticContent("/*", "core/modules/qhttp/testdata");

    start();
    std::string content;

    //----- test path normalization

    content = asioHttpGet("/css/../css/style.css", 200, "text/css");
    compareWithFile(content, "core/modules/qhttp/testdata/css/style.css");
    content = asioHttpGet("/css/./style.css", 200, "text/css");
    compareWithFile(content, "core/modules/qhttp/testdata/css/style.css");
    content = asioHttpGet("/././css/.././css/./../css/style.css", 200, "text/css");
    compareWithFile(content, "core/modules/qhttp/testdata/css/style.css");

    //----- test relative path containment

    content = asioHttpGet("/..", 401, "text/html");
    BOOST_TEST(content.find("401") != std::string::npos);
    content = asioHttpGet("/css/../..", 401, "text/html");
    BOOST_TEST(content.find("401") != std::string::npos);
}


BOOST_FIXTURE_TEST_CASE(handler_dispatch, QhttpFixture)
{
    auto testHandler = [](std::string const &name) {
        return [name](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
            resp->send(name + " " + printParams(req), "text/plain");
        };
    };

    server->addHandlers({
        {"GET",    "/api/v1/foos",           testHandler("Handler1")},
        {"POST",   "/api/v1/foos",           testHandler("Handler2")},
        {"PUT",    "/api/v1/bars",           testHandler("Handler3")},
        {"PATCH",  "/api/v1/bars",           testHandler("Handler4")},
        {"DELETE", "/api/v1/bars",           testHandler("Handler5")},
        {"GET",    "/api/v1/foos/:foo",      testHandler("Handler6")},
        {"GET",    "/api/v1/foos/:foo/:bar", testHandler("Handler7")}
    });

    start();

    CurlEasy curl;

    //----- Test basic handler dispatch by path and method

    curl.setup("GET", urlPrefix + "api/v1/foos", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[]");
    curl.setup("POST", urlPrefix + "api/v1/foos", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler2 params[] query[]");
    curl.setup("PUT", urlPrefix + "api/v1/bars", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler3 params[] query[]");
    curl.setup("PATCH", urlPrefix + "api/v1/bars", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler4 params[] query[]");
    curl.setup("DELETE", urlPrefix + "api/v1/bars", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler5 params[] query[]");

    //----- Test methods without installed handlers

    curl.setup("GET", urlPrefix + "api/v1/bars", "").perform().validate(404, "text/html");
    BOOST_TEST(curl.recdContent.find("404") != std::string::npos);
    curl.setup("PUT", urlPrefix + "api/v1/foos", "").perform().validate(404, "text/html");
    BOOST_TEST(curl.recdContent.find("404") != std::string::npos);

    //----- Test URL parameters

    curl.setup("GET", urlPrefix + "api/v1/foos?bar=baz", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[bar=baz]");
    curl.setup("GET", urlPrefix + "api/v1/foos?bar=bop&bar=baz&bip=bap", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[bar=baz,bip=bap]");

    //----- Test path captures

    curl.setup("GET", urlPrefix + "api/v1/foos/boz", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler6 params[foo=boz] query[]");
    curl.setup("GET", urlPrefix + "api/v1/foos/gleep/glorp", "").perform().validate(200, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler7 params[bar=glorp,foo=gleep] query[]");
}


BOOST_FIXTURE_TEST_CASE(ajax, QhttpFixture)
{
    auto ajax1 = server->addAjaxEndpoint("/ajax/foo");
    auto ajax2 = server->addAjaxEndpoint("/ajax/bar");

    start();

    CurlMulti m;

    //
    //----- ajaxHandler(c, r, n) is a factory that returns a handler which validates c, checks result
    //      content to be r, increments n, then resets result content and adds c back to m again.  This
    //      creates a handler chain that will keep turning around ajax requests, validating responses and
    //      incrementing the closed over counter on each iteration.
    //

    using Handler = std::function<void ()>;
    using HandlerFactory = std::function<Handler (CurlEasy& c, std::string const& r, int& n)>;

    HandlerFactory ajaxHandler = [&m, &ajaxHandler](CurlEasy& c, std::string const& r, int& n) {
        return [&m, &c, r, &n, &ajaxHandler]() {
            c.validate(200, "application/json");
            BOOST_TEST(c.recdContent == r);
            c.recdContent.erase();
            ++n;
            m.add(c, ajaxHandler(c, r, n));
        };
    };

    //
    //----- Set two client requests on one of the ajax endpoints, and one on the other.  Set up a counter
    //      and validation/turn-around handler for each on the libcurl multi-handle.
    //

    CurlEasy c1, c2, c3;

    c1.setup("GET", urlPrefix + "ajax/foo", "");
    c2.setup("GET", urlPrefix + "ajax/foo", "");
    c3.setup("GET", urlPrefix + "ajax/bar", "");

    int n1 = 0;
    int n2 = 0;
    int n3 = 0;

    m.add(c1, ajaxHandler(c1, "1", n1));
    m.add(c2, ajaxHandler(c2, "1", n2));
    m.add(c3, ajaxHandler(c3, "2", n3));

    //
    //----- Run the libcurl multi in this thread, for at most 225ms.  This will issue the initial client HTTP
    //      requests, but control should return after timeout with no response handlers run, since no updates
    //      have yet been pushed to the ajax endpoints.  Check that counts are all zero to confirm this.
    //

    m.perform(225);
    BOOST_TEST(n1 == 0);
    BOOST_TEST(n2 == 0);
    BOOST_TEST(n3 == 0);

    //
    //----- Start a thread that will push an update to the first ajax endpoint every 100ms, then run the
    //      libcurl multi in this thread for at most 225ms.  When we get control back, check via counters
    //      that both installed handlers for the first endpoint have run twice, and that the handler for
    //      the second endpoint has not been run erroneously.
    //

    std::atomic_bool stop{false};
    std::thread t1([&ajax1, &stop]() {
        while(!stop) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            ajax1->update("1");
        }
    });

    m.perform(225);
    BOOST_TEST(n1 == 2);
    BOOST_TEST(n2 == 2);
    BOOST_TEST(n3 == 0);

    //
    //----- Start an additional thread that will push an update to the second ajax endpoint every 100ms.
    //      Run the libcurl multi in this thread for at most 225ms.  When we get control back, check via
    //      counters that all three handlers have run two additional times.
    //

    std::thread t2([&ajax2, &stop]() {
        while(!stop) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            ajax2->update("2");
        }
    });

    m.perform(225);
    BOOST_TEST(n1 == 4);
    BOOST_TEST(n2 == 4);
    BOOST_TEST(n3 == 2);

    //----- Signal update threads to exit, and join them.

    stop = true;
    t1.join();
    t2.join();
}

}} // namespace lsst::qserv
