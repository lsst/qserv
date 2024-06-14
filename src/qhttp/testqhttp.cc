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
#include "boost/test/unit_test.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <getopt.h>
#include <set>
#include <sstream>
#include <stdio.h>
#include <string>
#include <thread>
#include <vector>

#include "boost/asio.hpp"
#include "boost/algorithm/string/join.hpp"
#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "boost/range/adaptors.hpp"
#include "curl/curl.h"

#include "lsst/log/Log.h"
#include "qhttp/MultiPartParser.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"
#include "qhttp/Status.h"

namespace asio = boost::asio;
namespace ip = boost::asio::ip;
namespace fs = boost::filesystem;

namespace {

void initMDC() { LOG_MDC("LWP", std::to_string(lsst::log::lwpID())); }

void compareWithFile(std::string const& content, std::string const& file) {
    std::ifstream f(file);
    BOOST_TEST(f.good());
    std::stringstream s;
    s << f.rdbuf();
    BOOST_TEST(s.str() == content);
}

std::string printParams(lsst::qserv::qhttp::Request::Ptr const& req) {
    std::map<std::string, std::string> pparams;
    for (auto const& pparam : req->params) {
        pparams[pparam.first] = pparam.first + "=" + pparam.second;
    }
    std::map<std::string, std::string> pquerys;
    for (auto const& pquery : req->query) {
        pquerys[pquery.first] = pquery.first + "=" + pquery.second;
    }
    return std::string("params[") + boost::join(pparams | boost::adaptors::map_values, ",") + "] " +
           "query[" + boost::join(pquerys | boost::adaptors::map_values, ",") + "]";
}

size_t writeToString(char* ptr, size_t size, size_t nmemb, void* userdata) {
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

class CurlEasy {
public:
    explicit CurlEasy(unsigned long numRetries_ = 1, unsigned long retryDelayMs_ = 1);
    ~CurlEasy();

    CurlEasy& setup(std::string const& method, std::string const& url, std::string const& data,
                    std::initializer_list<std::string> headers = {});

    CurlEasy& setupPostFormUpload(std::string const& url,
                                  std::unordered_map<std::string, std::string> const& parameters = {},
                                  std::unordered_map<std::string, std::string> const& files = {},
                                  std::initializer_list<std::string> headers = {});

    CurlEasy& perform();

    CurlEasy& validate(lsst::qserv::qhttp::Status responseCode, std::string const& contentType);

    unsigned long numRetries;
    unsigned long retryDelayMs;
    CURL* hcurl;
    curl_slist* hlist;
    curl_mime* hmultipart;
    std::string recdContent;

private:
    void _setHeaders(std::initializer_list<std::string> headers);
    void _setResponseHandler();
};

CurlEasy::CurlEasy(unsigned long numRetries_, unsigned long retryDelayMs_)
        : numRetries(numRetries_), retryDelayMs(retryDelayMs_) {
    hcurl = curl_easy_init();
    BOOST_TEST(hcurl != static_cast<CURL*>(nullptr));
    hlist = nullptr;
    hmultipart = nullptr;
}

CurlEasy::~CurlEasy() {
    curl_slist_free_all(hlist);
    curl_easy_cleanup(hcurl);
    curl_mime_free(hmultipart);
}

CurlEasy& CurlEasy::setup(std::string const& method, std::string const& url, std::string const& data,
                          std::initializer_list<std::string> headers) {
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
    _setHeaders(headers);
    _setResponseHandler();

    return *this;
}

CurlEasy& CurlEasy::setupPostFormUpload(std::string const& url,
                                        std::unordered_map<std::string, std::string> const& parameters,
                                        std::unordered_map<std::string, std::string> const& files,
                                        std::initializer_list<std::string> headers) {
    hmultipart = curl_mime_init(hcurl);
    BOOST_TEST(hmultipart != nullptr);
    curl_mimepart* part = nullptr;
    for (auto const& [name, val] : parameters) {
        part = curl_mime_addpart(hmultipart);
        BOOST_TEST(curl_mime_name(part, name.data()) == CURLE_OK);
        BOOST_TEST(curl_mime_data(part, val.data(), val.size()) == CURLE_OK);
    }
    for (auto const& [name, path] : files) {
        part = curl_mime_addpart(hmultipart);
        BOOST_TEST(curl_mime_name(part, name.data()) == CURLE_OK);
        BOOST_TEST(curl_mime_filedata(part, path.data()) == CURLE_OK);
    }
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_VERBOSE, 1L) == CURLE_OK);
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_URL, url.c_str()) == CURLE_OK);
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_MIMEPOST, hmultipart) == CURLE_OK);

    _setHeaders(headers);
    _setResponseHandler();

    return *this;
}

CurlEasy& CurlEasy::perform() {
    CURLcode ret;
    for (unsigned long i = 0; i < numRetries; ++i) {
        ret = curl_easy_perform(hcurl);
        if (ret != CURLE_SEND_ERROR) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(retryDelayMs));
    }
    BOOST_CHECK_EQUAL(ret, CURLE_OK);
    return *this;
}

CurlEasy& CurlEasy::validate(lsst::qserv::qhttp::Status responseCode, std::string const& contentType) {
    long recdResponseCode;
    char* recdContentType = nullptr;
    double recdContentLength;

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_RESPONSE_CODE, &recdResponseCode) == CURLE_OK);
    BOOST_TEST(recdResponseCode == responseCode);

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_CONTENT_TYPE, &recdContentType) == CURLE_OK);
    BOOST_TEST(recdContentType == contentType);

    BOOST_TEST(curl_easy_getinfo(hcurl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &recdContentLength) == CURLE_OK);
    BOOST_TEST(recdContentLength == recdContent.size());

    return *this;
}

void CurlEasy::_setHeaders(std::initializer_list<std::string> headers) {
    curl_slist_free_all(hlist);
    hlist = nullptr;
    for (auto& header : headers) {
        hlist = curl_slist_append(hlist, header.c_str());
    }
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_HTTPHEADER, hlist) == CURLE_OK);
}

void CurlEasy::_setResponseHandler() {
    recdContent.erase();
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_WRITEFUNCTION, writeToString) == CURLE_OK);
    BOOST_TEST(curl_easy_setopt(hcurl, CURLOPT_WRITEDATA, &recdContent) == CURLE_OK);
}

//
//----- CurlMutli is a helper class for managing multiple concurrent HTTP requests within a
//      single thread, using the libcurl "Multi" API.  Works with the "CurlEasy" class above.
//      http://curl.haxx.se/libcurl/c/libcurl-multi.html for API details.
//

class CurlMulti {
public:
    CurlMulti();
    ~CurlMulti();

    void add(CurlEasy& c, std::function<void()> const& handler);
    void perform(int msecs);

    CURLM* hcurlm;
    std::map<CURL*, std::function<void()>> handlers;
};

CurlMulti::CurlMulti() {
    hcurlm = curl_multi_init();
    BOOST_TEST(hcurlm != static_cast<CURLM*>(nullptr));
}

CurlMulti::~CurlMulti() { curl_multi_cleanup(hcurlm); }

void CurlMulti::add(CurlEasy& c, std::function<void()> const& handler) {
    handlers[c.hcurl] = handler;
    BOOST_TEST(curl_multi_add_handle(hcurlm, c.hcurl) == CURLM_OK);
}

void CurlMulti::perform(int msecs) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(msecs);
    while (std::chrono::steady_clock::now() < end) {
        int runningHandles = 0;
        BOOST_TEST(curl_multi_perform(hcurlm, &runningHandles) == CURLM_OK);

        int msgsInQueue;
        CURLMsg* msg;
        while ((msg = curl_multi_info_read(hcurlm, &msgsInQueue)) != nullptr) {
            BOOST_TEST(curl_multi_remove_handle(hcurlm, msg->easy_handle) == CURLM_OK);
            auto hit = handlers.find(msg->easy_handle);
            if (hit != handlers.end()) hit->second();
        }

        if (runningHandles == 0) return;

        int numfds = 0;
        auto remaining = end - std::chrono::steady_clock::now();
        msecs = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
        if (msecs > 0) {
            BOOST_TEST(curl_multi_wait(hcurlm, nullptr, 0, msecs, &numfds) == CURLM_OK);
        }
    }
}

}  // namespace

namespace lsst::qserv {

//
//----- The test fixture instantiates a qhttp server and a boost::asio::io_service to run it,
//      manages a thread that runs the io_service, and handles global init and cleanup of libcurl.
//

struct QhttpFixture {
    QhttpFixture() {
        server = qhttp::Server::create(service, 0);
        BOOST_TEST(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK);

        static char const* usage =
                "Usage: --client-threads=<num> --data=<path> --log-level=<level> --threads=<num> "
                "--retry-delay=<milliseconds>";
        static char const* opts = "d:l:t:r:m";
        static struct option lopts[] = {{"client-threads", required_argument, nullptr, 'c'},
                                        {"data", required_argument, nullptr, 'd'},
                                        {"log-level", required_argument, nullptr, 'l'},
                                        {"threads", required_argument, nullptr, 't'},
                                        {"retries", required_argument, nullptr, 'r'},
                                        {"retry-delay", required_argument, nullptr, 'm'},
                                        {nullptr, 0, nullptr, 0}};

        auto& argc = boost::unit_test::framework::master_test_suite().argc;
        auto& argv = boost::unit_test::framework::master_test_suite().argv;

        int opt;
        optind = 1;
        while ((opt = getopt_long(argc, argv, opts, lopts, nullptr)) != -1) {
            switch (opt) {
                case 'c':
                    try {
                        numClientThreads = std::min(static_cast<size_t>(std::thread::hardware_concurrency()),
                                                    std::max(numClientThreads, std::stoul(optarg)));
                    } catch (...) {
                        LOG_ERROR(usage);
                        std::abort();
                    }
                    break;
                case 'd':
                    dataDir = optarg;
                    break;
                case 'l':
                    logLevel = optarg;
                    break;
                case 't':
                    try {
                        numThreads = std::min(static_cast<size_t>(std::thread::hardware_concurrency()),
                                              std::max(numThreads, std::stoul(optarg)));
                    } catch (...) {
                        LOG_ERROR(usage);
                        std::abort();
                    }
                    break;
                case 'r':
                    try {
                        numRetries = std::max(numRetries, std::stoul(optarg));
                    } catch (...) {
                        LOG_ERROR(usage);
                        std::abort();
                    }
                    break;
                case 'm':
                    try {
                        retryDelayMs = std::max(retryDelayMs, std::stoul(optarg));
                    } catch (...) {
                        LOG_ERROR(usage);
                        std::abort();
                    }
                    break;
                default:
                    break;
            }
        }

        LOG_MDC_INIT(initMDC);
        LOG_CONFIG_PROP(std::string("log4j.rootLogger=") + logLevel +
                        ", CONSOLE\n"
                        "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n"
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n"
                        "log4j.appender.CONSOLE.layout.ConversionPattern="
                        "%d{yyyy-MM-ddTHH:mm:ss.SSSZ} LWP %-5X{LWP} %-5p %c{1} %m%n");
    }

    void start() {
        server->start();
        urlPrefix = "http://localhost:" + std::to_string(server->getPort()) + "/";
        for (size_t i = 0; i < numThreads; ++i) {
            serviceThreads.emplace_back([this]() {
                asio::io_service::work work(service);
                service.run();
            });
        }
    }

    ~QhttpFixture() {
        server->stop();
        service.stop();
        for (auto&& t : serviceThreads) {
            t.join();
        }
        curl_global_cleanup();
    }

    //
    //----- Use for the relative link tests below, which can't use libcurl, because libcurl snaps out dot
    //      pathname components on the client side.  This alternative sends a GET request and checks the
    //      reply using synchronous asio and regexps directly.
    //

    std::string asioHttpGet(std::string const& path, int responseCode, std::string const& contentType,
                            std::string const invalidContentLength = "") {
        boost::system::error_code ec;

        ip::tcp::endpoint endpoint(ip::address::from_string("127.0.0.1"), server->getPort());
        ip::tcp::socket socket(service);
        socket.connect(endpoint, ec);
        BOOST_TEST(!ec);

        std::string req = std::string("GET ") + path + " HTTP/1.1\r\n";
        if (!invalidContentLength.empty()) {
            req += std::string("Content-Length: ") + invalidContentLength + "\r\n";
        }
        req += "\r\n";

        asio::write(socket, asio::buffer(req), ec);
        BOOST_TEST(!ec);

        asio::streambuf respbuf;
        std::istream resp(&respbuf);
        size_t bytesRead = asio::read_until(socket, respbuf, "\r\n\r\n", ec);
        BOOST_TEST(!ec);
        size_t bytesBuffered = respbuf.size() - bytesRead;

        std::string line;
        std::map<std::string, std::string> header;
        static boost::regex respRe{"^[^ \\r]+ ([0-9]+) .*\\r$"};  // e.g. "HTTP/1.1 200 OK"
        boost::smatch respMatch;
        if (getline(resp, line) && boost::regex_match(line, respMatch, respRe)) {
            BOOST_TEST(stoi(respMatch[1].str()) == responseCode);
            static boost::regex headerRe{"^([^:\\r]+): ?([^\\r]*)\\r$"};  // e.g. "header: value"
            boost::smatch headerMatch;
            while (getline(resp, line) && boost::regex_match(line, headerMatch, headerRe)) {
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

    void testStaticContent() {
        //----- test invalid root directory

        BOOST_CHECK_THROW(server->addStaticContent("/*", "/doesnotexist"), fs::filesystem_error);
        BOOST_CHECK_THROW(server->addStaticContent("/*", dataDir + "index.html"), fs::filesystem_error);

        //----- set up valid static content for subsequent tests

        server->addStaticContent("/*", dataDir);
        start();

        CurlEasy curl(numRetries, retryDelayMs);

        //----- test default index.html

        curl.setup("GET", urlPrefix, "").perform().validate(qhttp::STATUS_OK, "text/html");
        compareWithFile(curl.recdContent, dataDir + "index.html");

        //----- test subdirectories and file typing by extension

        curl.setup("GET", urlPrefix + "css/style.css", "").perform().validate(qhttp::STATUS_OK, "text/css");
        compareWithFile(curl.recdContent, dataDir + "css/style.css");
        curl.setup("GET", urlPrefix + "images/lsst.gif", "")
                .perform()
                .validate(qhttp::STATUS_OK, "image/gif");
        compareWithFile(curl.recdContent, dataDir + "images/lsst.gif");
        curl.setup("GET", urlPrefix + "images/lsst.jpg", "")
                .perform()
                .validate(qhttp::STATUS_OK, "image/jpeg");
        compareWithFile(curl.recdContent, dataDir + "images/lsst.jpg");
        curl.setup("GET", urlPrefix + "images/lsst.png", "")
                .perform()
                .validate(qhttp::STATUS_OK, "image/png");
        compareWithFile(curl.recdContent, dataDir + "images/lsst.png");
        curl.setup("GET", urlPrefix + "js/main.js", "")
                .perform()
                .validate(qhttp::STATUS_OK, "application/javascript");
        compareWithFile(curl.recdContent, dataDir + "js/main.js");

        //----- test redirect for directory w/o trailing "/"

        char* redirect = nullptr;
        curl.setup("GET", urlPrefix + "css", "").perform().validate(qhttp::STATUS_MOVED_PERM, "text/html");
        BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_MOVED_PERM)) != std::string::npos);
        BOOST_TEST(curl_easy_getinfo(curl.hcurl, CURLINFO_REDIRECT_URL, &redirect) == CURLE_OK);
        BOOST_TEST(redirect == urlPrefix + "css/");

        //----- test non-existent file

        curl.setup("GET", urlPrefix + "doesNotExist", "")
                .perform()
                .validate(qhttp::STATUS_NOT_FOUND, "text/html");
        BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_NOT_FOUND)) != std::string::npos);
    }

    asio::io_service service;
    std::vector<std::thread> serviceThreads;
    qhttp::Server::Ptr server;
    std::string urlPrefix;
    std::string dataDir;
    std::string logLevel = "DEBUG";
    unsigned long numRetries = 1;
    unsigned long retryDelayMs = 1;
    size_t numThreads = 1;
    size_t numClientThreads = 1;
};

BOOST_FIXTURE_TEST_CASE(request_timeout, QhttpFixture) {
    //----- set up server with a handler on "/" and a request timeout of 20ms

    server->addHandler("GET", "/", [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
        resp->sendStatus(qhttp::STATUS_OK);
    });

    server->setRequestTimeout(std::chrono::milliseconds(20));
    start();

    //----- verify able to connect to the server

    boost::system::error_code ec;

    ip::tcp::endpoint endpoint(ip::address::from_string("127.0.0.1"), server->getPort());
    ip::tcp::socket socket(service);
    socket.connect(endpoint, ec);
    BOOST_TEST(!ec);

    //----- sleep long enough for request timeout to expire

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    //----- write the request (should still succeed after timeout)

    std::string req = std::string("GET / HTTP/1.1\r\n\r\n");
    asio::write(socket, asio::buffer(req), ec);
    BOOST_TEST(!ec);

    //----- attempt to read response (should fail after timeout)
    //
    //      Note: previously this test checked for ec == EOF.  As it turns out, boost::asio guarantees
    //      only a zero return value from read_until() on error, and not any particular error codes.
    //

    asio::streambuf respbuf;
    size_t num_read_after_timeout = asio::read_until(socket, respbuf, "\r\n\r\n", ec);
    BOOST_TEST(num_read_after_timeout == (size_t)0);
}

BOOST_FIXTURE_TEST_CASE(shutdown, QhttpFixture) {
    //----- set up server with a handler on "/" that counts invocations

    int invocations = 0;

    server->addHandler("GET", "/", [&invocations](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
        ++invocations;
        resp->sendStatus(qhttp::STATUS_OK);
    });

    //----- start, and verify handler invoked

    start();
    CurlEasy curl1(numRetries, retryDelayMs);
    curl1.setup("GET", urlPrefix, "").perform().validate(qhttp::STATUS_OK, "text/html");
    BOOST_TEST(invocations == 1);

    //----- shutdown, and verify cannot connect.  Check on both existing curl object (already open
    //      HTTP 1.1 connection) and new curl object (fresh connection)

    server->stop();
    curl1.setup("GET", urlPrefix, "");
    BOOST_TEST(curl_easy_perform(curl1.hcurl) == CURLE_COULDNT_CONNECT);
    CurlEasy curl2(numRetries, retryDelayMs);
    curl2.setup("GET", urlPrefix, "");
    BOOST_TEST(curl_easy_perform(curl2.hcurl) == CURLE_COULDNT_CONNECT);

    //----- restart, and verify handler in invoked again

    server->start();
    curl1.setup("GET", urlPrefix, "").perform().validate(qhttp::STATUS_OK, "text/html");
    BOOST_TEST(invocations == 2);
    curl2.setup("GET", urlPrefix, "").perform().validate(qhttp::STATUS_OK, "text/html");
    BOOST_TEST(invocations == 3);
}

BOOST_FIXTURE_TEST_CASE(case_insensitive_headers, QhttpFixture) {
    //----- server with handler that checks for same header in multiple cases

    server->addHandler("GET", "/", [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
        if ((req->header["foobar"] == "baz") && (req->header["FOOBAR"] == "baz") &&
            (req->header["FooBar"] == "baz")) {
            resp->sendStatus(qhttp::STATUS_OK);
        } else {
            resp->sendStatus(qhttp::STATUS_INTERNAL_SERVER_ERR);
        }
    });

    start();
    CurlEasy curl(numRetries, retryDelayMs);

    //----- tests provide same header in multiple cases

    curl.setup("GET", urlPrefix, "", {"foobar: baz"}).perform().validate(qhttp::STATUS_OK, "text/html");
    curl.setup("GET", urlPrefix, "", {"FOOBAR: baz"}).perform().validate(qhttp::STATUS_OK, "text/html");
}

BOOST_FIXTURE_TEST_CASE(percent_decoding, QhttpFixture) {
    //----- server with handlers to catch potential encoded "/" dispatch error
    //      and param echoing to check param decode

    server->addHandler("GET", R"(/path-with-/-and-\?)",
                       [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
                           resp->send("percent-encoded '/' dispatch error", "text/plain");
                       });

    server->addHandler("GET", R"(/path-with-\/-and-\?)",
                       [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
                           resp->send(printParams(req), "text/plain");
                       });

    start();
    CurlEasy curl(numRetries, retryDelayMs);

    //----- send in request with percent encodes and check echoed params

    curl.setup("GET", urlPrefix + "path%2Dwith%2d%2F-and-%3F?key-with-%3D=value-with-%26&key2=value2", "");
    curl.perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "params[] query[key-with-==value-with-&,key2=value2]");
}

BOOST_FIXTURE_TEST_CASE(static_content, QhttpFixture) { testStaticContent(); }

BOOST_FIXTURE_TEST_CASE(static_content_small_buf, QhttpFixture) {
    //----- set a tiny buffer size for sending responses to evaluate an ability
    //      of the implementation to break the response into multiple messages.

    server->setMaxResponseBufSize(128);

    //----- after that repeat the static content reading test

    testStaticContent();
}

BOOST_FIXTURE_TEST_CASE(relative_url_containment, QhttpFixture) {
    server->addStaticContent("/*", dataDir);

    start();
    std::string content;

    //----- test path normalization

    content = asioHttpGet("/css/../css/style.css", qhttp::STATUS_OK, "text/css");
    compareWithFile(content, dataDir + "css/style.css");
    content = asioHttpGet("/css/./style.css", qhttp::STATUS_OK, "text/css");
    compareWithFile(content, dataDir + "css/style.css");
    content = asioHttpGet("/././css/.././css/./../css/style.css", qhttp::STATUS_OK, "text/css");
    compareWithFile(content, dataDir + "css/style.css");

    //----- test relative path containment

    content = asioHttpGet("/..", qhttp::STATUS_FORBIDDEN, "text/html");
    BOOST_TEST(content.find(std::to_string(qhttp::STATUS_FORBIDDEN)) != std::string::npos);
    content = asioHttpGet("/css/../..", qhttp::STATUS_FORBIDDEN, "text/html");
    BOOST_TEST(content.find(std::to_string(qhttp::STATUS_FORBIDDEN)) != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(exception_handling, QhttpFixture) {
    boost::system::error_code ec;
    std::string content;

    server->addStaticContent("/etc/*", "/etc/");

    server->addHandler("GET", "/throw/:errno", [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
        int ev = std::stoi(req->params["errno"]);  // will throw if can't parse int
        throw(boost::system::system_error(ev, boost::system::generic_category()));
    });

    server->addHandler("GET", "/throw-after-send", [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
        resp->sendStatus(qhttp::STATUS_OK);
        throw std::runtime_error("test");
    });

    server->addHandler(
            "GET", "/invalid-content-length",
            [](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) { resp->sendStatus(qhttp::STATUS_OK); });

    start();

    CurlEasy curl(numRetries, retryDelayMs);

    //----- test EACCESS thrown from static file handler

    curl.setup("GET", urlPrefix + "etc/shadow", "").perform().validate(qhttp::STATUS_FORBIDDEN, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_FORBIDDEN)) != std::string::npos);

    //----- test exceptions thrown from user handler

    curl.setup("GET", urlPrefix + (boost::format("throw/%1%") % EACCES).str(), "");
    curl.perform().validate(qhttp::STATUS_FORBIDDEN, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_FORBIDDEN)) != std::string::npos);

    curl.setup("GET", urlPrefix + (boost::format("throw/%1%") % ENOENT).str(), "");
    curl.perform().validate(qhttp::STATUS_INTERNAL_SERVER_ERR, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_INTERNAL_SERVER_ERR)) != std::string::npos);

    curl.setup("GET", urlPrefix + "throw/make-stoi-throw-invalid-argument", "");
    curl.perform().validate(qhttp::STATUS_INTERNAL_SERVER_ERR, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_INTERNAL_SERVER_ERR)) != std::string::npos);

    //----- Test exception thrown in user handler after calling a request send() method.  This would be a user
    //      programming error, but we defend against it anyway.  From the point of view of the HTTP client,
    //      the response provided by the handler before the exception goes through.

    curl.setup("GET", urlPrefix + "throw-after-send", "").perform().validate(qhttp::STATUS_OK, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_OK)) != std::string::npos);

    //----- test resource path with embedded null

    curl.setup("GET", urlPrefix + "etc/%00/", "").perform().validate(qhttp::STATUS_BAD_REQ, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_BAD_REQ)) != std::string::npos);

    content = asioHttpGet(std::string("/\0/", 3), qhttp::STATUS_BAD_REQ, "text/html");
    BOOST_TEST(content.find(std::to_string(qhttp::STATUS_BAD_REQ)) != std::string::npos);

    //----- test request with invalid Content-Length headers

    content = asioHttpGet("/invalid-content-length", qhttp::STATUS_BAD_REQ, "text/html", "not-an-integer");
    BOOST_TEST(content.find(std::to_string(qhttp::STATUS_BAD_REQ)) != std::string::npos);

    content = asioHttpGet("/invalid-content-length", qhttp::STATUS_BAD_REQ, "text/html",
                          "18446744073709551616");
    BOOST_TEST(content.find(std::to_string(qhttp::STATUS_BAD_REQ)) != std::string::npos);
}

BOOST_FIXTURE_TEST_CASE(handler_dispatch, QhttpFixture) {
    auto testHandler = [](std::string const& name) {
        return [name](qhttp::Request::Ptr req, qhttp::Response::Ptr resp) {
            resp->send(name + " " + printParams(req), "text/plain");
        };
    };

    server->addHandlers({{"GET", "/api/v1/foos", testHandler("Handler1")},
                         {"POST", "/api/v1/foos", testHandler("Handler2")},
                         {"PUT", "/api/v1/bars", testHandler("Handler3")},
                         {"PATCH", "/api/v1/bars", testHandler("Handler4")},
                         {"DELETE", "/api/v1/bars", testHandler("Handler5")},
                         {"GET", "/api/v1/foos/:foo", testHandler("Handler6")},
                         {"GET", "/api/v1/foos/:foo/:bar", testHandler("Handler7")}});

    start();

    CurlEasy curl(numRetries, retryDelayMs);

    //----- Test basic handler dispatch by path and method

    curl.setup("GET", urlPrefix + "api/v1/foos", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[]");
    curl.setup("POST", urlPrefix + "api/v1/foos", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler2 params[] query[]");
    curl.setup("PUT", urlPrefix + "api/v1/bars", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler3 params[] query[]");
    curl.setup("PATCH", urlPrefix + "api/v1/bars", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler4 params[] query[]");
    curl.setup("DELETE", urlPrefix + "api/v1/bars", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler5 params[] query[]");

    //----- Test methods without installed handlers

    curl.setup("GET", urlPrefix + "api/v1/bars", "").perform().validate(qhttp::STATUS_NOT_FOUND, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_NOT_FOUND)) != std::string::npos);
    curl.setup("PUT", urlPrefix + "api/v1/foos", "").perform().validate(qhttp::STATUS_NOT_FOUND, "text/html");
    BOOST_TEST(curl.recdContent.find(std::to_string(qhttp::STATUS_NOT_FOUND)) != std::string::npos);

    //----- Test URL parameters

    curl.setup("GET", urlPrefix + "api/v1/foos?bar=baz", "")
            .perform()
            .validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[bar=baz]");
    curl.setup("GET", urlPrefix + "api/v1/foos?bar=bop&bar=baz&bip=bap", "")
            .perform()
            .validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler1 params[] query[bar=baz,bip=bap]");

    //----- Test path captures

    curl.setup("GET", urlPrefix + "api/v1/foos/boz", "").perform().validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler6 params[foo=boz] query[]");
    curl.setup("GET", urlPrefix + "api/v1/foos/gleep/glorp", "")
            .perform()
            .validate(qhttp::STATUS_OK, "text/plain");
    BOOST_TEST(curl.recdContent == "Handler7 params[bar=glorp,foo=gleep] query[]");
}

BOOST_FIXTURE_TEST_CASE(ajax, QhttpFixture) {
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

    using Handler = std::function<void()>;
    using HandlerFactory = std::function<Handler(CurlEasy & c, std::string const& r, int& n)>;

    HandlerFactory ajaxHandler = [&m, &ajaxHandler](CurlEasy& c, std::string const& r, int& n) {
        return [&m, &c, r, &n, &ajaxHandler]() {
            c.validate(qhttp::STATUS_OK, "application/json");
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

    CurlEasy c1(numRetries, retryDelayMs), c2(numRetries, retryDelayMs), c3(numRetries, retryDelayMs);

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
    //----- Start a thread that will push an two updates to the first ajax endpoint, separated by 100ms.
    //      Run the libcurl multi in this thread for at least 25ms after the last update.  Check via counters
    //      that both installed handlers for the first endpoint have run twice, and that the handler for
    //      the second endpoint has not been run erroneously.
    //

    std::atomic<bool> done1{false};
    std::thread t1([&ajax1, &done1]() {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax1->update("1");
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax1->update("1");
        done1 = true;
    });

    while (!done1) m.perform(25);
    m.perform(25);

    BOOST_TEST(n1 == 2);
    BOOST_TEST(n2 == 2);
    BOOST_TEST(n3 == 0);

    //
    //----- Start threads that will push two additional updates to both ajax endpoints, separated by 100ms.
    //      Run the libcurl multi in this thread for at least 25ms after the last update.  Check via
    //      counters that all three handlers have run two additional times.
    //

    std::atomic<bool> done2{false};
    std::thread t2([&ajax1, &done2]() {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax1->update("1");
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax1->update("1");
        done2 = true;
    });

    std::atomic<bool> done3{false};
    std::thread t3([&ajax2, &done3]() {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax2->update("2");
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        ajax2->update("2");
        done3 = true;
    });

    while (!done2 || !done3) m.perform(25);
    m.perform(25);

    BOOST_TEST(n1 == 4);
    BOOST_TEST(n2 == 4);
    BOOST_TEST(n3 == 2);

    //----- Join exited threads

    t1.join();
    t2.join();
    t3.join();
}

BOOST_FIXTURE_TEST_CASE(body_reader, QhttpFixture) {
    // Note that the completion status sending is delayed when the body reading
    // is asynchronous, and is triggered explicitly by the handler.
    auto makeTestHandler = [](bool autoReadEntireBody, std::string const& expectedContent) {
        return [autoReadEntireBody, &expectedContent](auto request, auto response) {
            BOOST_CHECK_EQUAL(request->contentLengthBytes(), expectedContent.size());
            if (autoReadEntireBody) {
                BOOST_CHECK_EQUAL(request->contentLengthBytes(), request->contentReadBytes());
                BOOST_CHECK_EQUAL(request->contentReadBytes(), expectedContent.size());
                std::string content;
                request->content >> content;
                BOOST_CHECK_EQUAL(content, expectedContent);
                response->sendStatus(qhttp::STATUS_OK);
            } else {
                request->readEntireBodyAsync(
                        [&expectedContent](auto request, auto response, bool success, std::size_t bytesRead) {
                            BOOST_CHECK_EQUAL(success, true);
                            if (success) {
                                BOOST_CHECK_EQUAL(request->contentReadBytes(), expectedContent.size());
                                // Some (in the extreme case - all) bytes might be already read when
                                // the request header was received and processed.
                                BOOST_TEST(bytesRead <= request->contentReadBytes());
                            }
                            std::string content;
                            request->content >> content;
                            BOOST_CHECK_EQUAL(content, expectedContent);
                            response->sendStatus(qhttp::STATUS_OK);
                        });
            }
        };
    };

    bool const readEntireBody = true;
    std::string const content = "abc";
    server->addHandlers({{"POST", "/foo1", makeTestHandler(readEntireBody, content)}});
    server->addHandlers({{"POST", "/foo2", makeTestHandler(!readEntireBody, content), !readEntireBody}});

    start();

    CurlEasy curl(numRetries, retryDelayMs);

    for (std::size_t i = 0; i < 10; ++i) {
        curl.setup("POST", urlPrefix + "foo1", content).perform().validate(qhttp::STATUS_OK, "text/html");
        curl.setup("POST", urlPrefix + "foo2", content).perform().validate(qhttp::STATUS_OK, "text/html");
    }
}

BOOST_FIXTURE_TEST_CASE(body_stream_reader, QhttpFixture) {
    class RequestHandler : public std::enable_shared_from_this<RequestHandler>, boost::noncopyable {
    public:
        static void handle(std::size_t expectedNumReads, std::string const& expectedContent,
                           qhttp::Request::Ptr request, qhttp::Response::Ptr response,
                           std::size_t bytesToRead = 0) {
            auto handler = std::shared_ptr<RequestHandler>(
                    new RequestHandler(expectedNumReads, expectedContent, bytesToRead));
            handler->_handle(request, response);
        }

    private:
        RequestHandler(std::size_t expectedNumReads, std::string const& expectedContent,
                       std::size_t bytesToRead = 0)
                : _expectedNumReads(expectedNumReads),
                  _expectedContent(expectedContent),
                  _bytesToRead(bytesToRead) {}

        void _handle(qhttp::Request::Ptr request, qhttp::Response::Ptr response) {
            BOOST_CHECK_EQUAL(request->contentLengthBytes(), _expectedContent.size());
            BOOST_TEST(request->contentReadBytes() <= request->contentLengthBytes());
            request->readPartialBodyAsync(
                    [self = shared_from_this()](auto request, auto response, bool success,
                                                std::size_t bytesRead) {
                        if (success) {
                            if (self->_bytesToRead == 0) {
                                BOOST_TEST(bytesRead <= request->recordSizeBytes());
                            } else {
                                BOOST_TEST(bytesRead <= self->_bytesToRead);
                            }
                        }
                        self->_processData(request, response, success);
                    },
                    _bytesToRead);
        }
        void _processData(qhttp::Request::Ptr request, qhttp::Response::Ptr response, bool success) {
            BOOST_TEST(request->contentReadBytes() <= request->contentLengthBytes());
            BOOST_CHECK_EQUAL(success, true);
            if (!success) {
                response->sendStatus(qhttp::STATUS_INTERNAL_SERVER_ERR);
                return;
            }
            _readContent.append(std::istreambuf_iterator<char>(request->content), {});
            ++_numReads;
            if (request->contentReadBytes() == request->contentLengthBytes()) {
                BOOST_CHECK_EQUAL(_expectedNumReads, _numReads);
                BOOST_CHECK_EQUAL(_expectedContent.size(), _readContent.size());
                BOOST_CHECK_EQUAL(_expectedContent, _readContent);
                response->sendStatus(qhttp::STATUS_OK);
            } else {
                request->readPartialBodyAsync(
                        [self = shared_from_this()](auto request, auto response, bool success,
                                                    std::size_t bytesRead) {
                            if (success) {
                                if (self->_bytesToRead == 0) {
                                    BOOST_TEST(bytesRead <= request->recordSizeBytes());
                                } else {
                                    BOOST_TEST(bytesRead <= self->_bytesToRead);
                                }
                            }
                            self->_processData(request, response, success);
                        },
                        _bytesToRead);
            }
        }
        std::size_t const _expectedNumReads;
        std::string const& _expectedContent;
        std::size_t const _bytesToRead = 0;
        std::size_t _numReads = 0;
        std::string _readContent;
    };

    BOOST_CHECK_EQUAL(qhttp::Request::defaultRecordSizeBytes, 1024 * 1024);
    std::string const expectedContent(16 * qhttp::Request::defaultRecordSizeBytes, '0');

    bool const readEntireBody = true;
    server->addHandlers({{"POST", "/foo",
                          [expectedNumReads = 16, &expectedContent](auto request, auto response) {
                              RequestHandler::handle(expectedNumReads, expectedContent, request, response);
                          },
                          !readEntireBody}});

    server->addHandlers({{"POST", "/bar",
                          [expectedNumReads = 16 * 1024, &expectedContent](auto request, auto response) {
                              std::size_t const bytesToRead = 1024;
                              RequestHandler::handle(expectedNumReads, expectedContent, request, response,
                                                     bytesToRead);
                          },
                          !readEntireBody}});

    start();

    std::vector<std::thread> threads;
    for (std::size_t i = 0; i < numClientThreads; ++i) {
        threads.emplace_back([this, &expectedContent]() {
            for (std::string const path : {"foo", "bar"}) {
                CurlEasy curl(numRetries, retryDelayMs);
                curl.setup("POST", urlPrefix + path, expectedContent)
                        .perform()
                        .validate(qhttp::STATUS_OK, "text/html");
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

class TestRequestProcessor : public qhttp::RequestProcessor {
public:
    explicit TestRequestProcessor(qhttp::Response::Ptr response,
                                  std::unordered_map<std::string, std::string> const& expectedParameters,
                                  std::unordered_map<std::string, std::string> const& expectedFiles)
            : qhttp::RequestProcessor(response),
              _expectedParameters(expectedParameters),
              _expectedFiles(expectedFiles) {}

    virtual bool onParamValue(qhttp::ContentHeader const& hdr, std::string const& name,
                              std::string_view const& value) {
        BOOST_CHECK(_receivedParameters.count(name) == 0);
        BOOST_CHECK(_expectedParameters.count(name) != 0);
        BOOST_CHECK_EQUAL(_expectedParameters.at(name), value);
        _receivedParameters[name] = value;
        return true;
    }
    virtual bool onFileOpen(qhttp::ContentHeader const& hdr, std::string const& name,
                            std::string const& filename, std::string const& contentType) {
        BOOST_CHECK(_receivedFilesContent.count(name) == 0);
        BOOST_CHECK(_expectedFiles.count(name) != 0);
        _currentFile = name;
        _receivedFilesContent[name] = std::string();
        return true;
    }
    virtual bool onFileContent(std::string_view const& data) {
        BOOST_CHECK(!_currentFile.empty());
        BOOST_CHECK(_receivedFilesContent.count(_currentFile) != 0);
        BOOST_CHECK(_expectedFiles.count(_currentFile) != 0);
        _receivedFilesContent[_currentFile] += data;
        return true;
    }
    virtual bool onFileClose() {
        BOOST_CHECK(!_currentFile.empty());
        BOOST_CHECK(_receivedFilesContent.count(_currentFile) != 0);
        BOOST_CHECK(_expectedFiles.count(_currentFile) != 0);
        compareWithFile(_receivedFilesContent[_currentFile], _expectedFiles.at(_currentFile));
        return true;
    }
    virtual void onFinished(std::string const& error) {
        if (error.empty()) {
            BOOST_CHECK_EQUAL(_receivedParameters.size(), _expectedParameters.size());
            BOOST_CHECK_EQUAL(_receivedFilesContent.size(), _expectedFiles.size());
            response->sendStatus(qhttp::STATUS_OK);
        } else {
            response->sendStatus(qhttp::STATUS_INTERNAL_SERVER_ERR);
        }
    }

private:
    std::string _currentFile;
    std::unordered_map<std::string, std::string> _receivedParameters;
    std::unordered_map<std::string, std::string> _receivedFilesContent;
    std::unordered_map<std::string, std::string> const _expectedParameters;
    std::unordered_map<std::string, std::string> const _expectedFiles;
};

BOOST_FIXTURE_TEST_CASE(multi_part, QhttpFixture) {
    std::unordered_map<std::string, std::string> const parameters = {{"p1", "v1"}, {"p2", "v2"}, {"p3", ""}};
    std::unordered_map<std::string, std::string> const files = {{"stype", dataDir + "css/style.css"},
                                                                {"script", dataDir + "js/main.js"}};
    bool const readEntireBody = true;
    server->addHandlers({{"POST", "/foo",
                          [&parameters, &files](auto request, auto response) {
                              auto processor = make_shared<TestRequestProcessor>(response, parameters, files);
                              qhttp::MultiPartParser::parse(request, processor);
                          },
                          readEntireBody}});
    server->addHandlers({{"POST", "/bar",
                          [&parameters, &files](auto request, auto response) {
                              auto processor = make_shared<TestRequestProcessor>(response, parameters, files);
                              qhttp::MultiPartParser::parse(request, processor);
                          },
                          !readEntireBody}});

    start();

    for (std::string const service : {"foo", "bar"}) {
        CurlEasy curl(numRetries, retryDelayMs);
        curl.setupPostFormUpload(urlPrefix + service, parameters, files,
                                 {"Content-Type: multipart/form-data"})
                .perform()
                .validate(qhttp::STATUS_OK, "text/html");
    }
}

}  // namespace lsst::qserv
