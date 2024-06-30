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
#include "replica/apps/HttpLibServerApp.h"

// System headers
#include <chrono>
#include <iostream>

// Third party headers
#include <httplib.h>

// Qserv headers
#include "util/TimeUtils.h"

using namespace std;
namespace util = lsst::qserv::util;

namespace {
string const description =
        "This application runs an embedded HTTP server based on 'cpp-httplib' for a purpose"
        "  of testing the server's performance, scalability and stability.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

/// @return 'YYYY-MM-DD HH:MM:SS.mmm  '
string timestamp() {
    return util::TimeUtils::toDateTimeString(chrono::milliseconds(util::TimeUtils::now())) + "  ";
}

string headers2string(httplib::Headers const& headers) {
    string s;
    for (auto const& [key, val] : headers) {
        s += key + ": " + val + "\n";
    }
    return s;
}

void logger(httplib::Request const& req, httplib::Response const& res, bool dumpRequestBody,
            bool dumpResponseBody) {
    string query;
    for (auto const& [param, val] : req.params) {
        query += (query.empty() ? "?" : "&") + param + "=" + val;
    }
    cout << "=== REQUEST [HEADER] ===\n"
         << req.method << " " + req.version << " " + req.path << query << "\n"
         << headers2string(req.headers);
    if (dumpRequestBody) {
        cout << "=== REQUEST [BODY] ===\n";
        cout << req.body << "\n";
    }
    cout << "--- RESPONSE [HEADER] ---\n"
         << res.status << " " << res.version << "\n"
         << headers2string(res.headers) << "\n";
    if (dumpResponseBody) {
        cout << "--- RESPONSE [BODY] ---\n";
        cout << res.body << "\n";
    }
    cout << endl;
}

void dump_multipart_file(httplib::MultipartFormData const& file) {
    cout << "----------------------------------------------------------\n"
         << "name: '" << file.name << "'\n"
         << "filename: '" << file.filename << "'\n"
         << "content_type: '" << file.content_type << "\n"
         << "content.size(): " << file.content.size() << "\n"
         << "content:\n"
         << "'" << file.content << "'\n"
         << endl;
}

}  // namespace

namespace lsst::qserv::replica {

HttpLibServerApp::Ptr HttpLibServerApp::create(int argc, char* argv[]) {
    return Ptr(new HttpLibServerApp(argc, argv));
}

HttpLibServerApp::HttpLibServerApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().option("port",
                    "The port number for listening for incoming connections. Specifying the number "
                    "of 0 will result in allocating the next available port.",
                    _port)
            .option("bind-addr", "An address to bind the server to.", _bindAddr)
            .option("num-threads",
                    "The number of threads to run the server. The number of 0 means "
                    "the number of the threads will be equal to the number of the CPU cores.",
                    _numThreads)
            .option("max-queued-requests",
                    "The parameter limiting the maximum number of pending requests, i.e. requests "
                    "accept()ed by the listener but still waiting to be serviced by worker threads. "
                    "Default limit is 0 (unlimited). Once the limit is reached, the listener will "
                    "shutdown the client connection.",
                    _maxQueuedRequests)
            .option("data-dir",
                    "A location of the data directory where the test files parsed "
                    "in bodies of the multpart requests will be saved.",
                    _dataDir)
            .option("message-size-bytes",
                    "The size of the message to be sent in the response body. Must be greater than 0.",
                    _messageSizeBytes)
            .option("report-interval-ms",
                    "An interval (milliseconds) for reporting the progress counters. Must be greater than "
                    "0.",
                    _reportIntervalMs)
            .flag("progress",
                  "The flag which would turn on periodic progress report on the incoming requests.",
                  _progress)
            .flag("verbose", "The flag which would turn on detailed report on the incoming requests.",
                  _verbose)
            .flag("verbose-dump-request-body",
                  "The flag which would turn on dumping the request body in the verbose mode.",
                  _verboseDumpRequestBody)
            .flag("verbose-dump-response-body",
                  "The flag which would turn on dumping the response body in the verbose mode.",
                  _verboseDumpResponseBody);
}

int HttpLibServerApp::runImpl() {
    if (_dataDir.back() != '/') _dataDir += '/';

    string data;
    data.resize(_messageSizeBytes, '0');

    httplib::Server svr;
    if (!svr.is_valid()) {
        cerr << "Failed to create the server." << endl;
        return 1;
    }
    svr.Get("/", [](auto const& req, auto& res) { res.set_redirect("/data"); });
    atomic<size_t> count{0};
    svr.Get("/data", [&data, &count](auto const& req, auto& res) {
        res.set_content(data, "text/plain");
        count++;
    });
    svr.Get("/slow", [](auto const& req, auto& res) {
        this_thread::sleep_for(chrono::seconds(2));
        res.set_content("Slow...\n", "text/plain");
    });
    svr.Get("/dump",
            [](auto const& req, auto& res) { res.set_content(headers2string(req.headers), "text/plain"); });
    svr.Get("/stop", [&](auto const& req, auto& resp) { svr.stop(); });
    svr.Get(R"(/numbers/(\d+))", [&](auto const& req, auto& res) {
        auto numbers = req.matches[1];
        res.set_content(numbers, "text/plain");
    });
    svr.Get("/users/:id", [&](auto const& req, auto& res) {
        auto user_id = req.path_params.at("id");
        res.set_content(user_id, "text/plain");
    });
    svr.Post("/multipart", [&](auto const& req, auto& resp) {
        cout << "/multipart\n"
             << " is_multipart_form_data: " << (req.is_multipart_form_data() ? "1" : "0") << "\n"
             << "  files.size(): " << req.files.size() << "\n"
             << "  has_file(\"style\"): " << (req.has_file("style") ? "1" : "0") << endl;
        for (auto const& [name, file] : req.files) {
            ::dump_multipart_file(file);
        }
    });
    svr.Post("/content_receiver",
             [&](auto const& req, auto& res, httplib::ContentReader const& content_reader) {
                 if (req.is_multipart_form_data()) {
                     httplib::MultipartFormDataItems files;
                     content_reader(
                             [&](httplib::MultipartFormData const& file) {
                                 files.push_back(file);
                                 return true;
                             },
                             [&](char const* data, size_t data_length) {
                                 files.back().content.append(data, data_length);
                                 return true;
                             });
                     for (auto const& file : files) {
                         ::dump_multipart_file(file);
                     }
                 } else {
                     string body;
                     content_reader([&](char const* data, size_t data_length) {
                         body.append(data, data_length);
                         return true;
                     });
                 }
             });
    svr.Post("/save_content", [&](auto const& req, auto& res, httplib::ContentReader const& content_reader) {
        if (!req.is_multipart_form_data()) {
            res.status = 400;
            return;
        }
        string filename;
        ofstream fs;
        auto const close = [&]() {
            if (fs.is_open()) {
                cout << "Close: " << filename << endl;
                fs.flush();
                fs.close();
            }
        };
        auto const open = [&](string const& newFilename) -> bool {
            close();
            filename = newFilename;
            if (!filename.empty()) {
                cout << "Open:  " << filename << endl;
                fs.open(filename, ios::binary);
                if (!fs.is_open()) {
                    cerr << "Failed to open file: " << filename << endl;
                    return false;
                }
            }
            return true;
        };
        auto const write = [&](char const* data, size_t data_length) -> bool {
            if (!filename.empty()) {
                cout << "Write: " << filename << " (" << data_length << " bytes)" << endl;
                if (!fs.write(data, data_length)) {
                    cerr << "Failed to write into: " << filename << endl;
                    return false;
                }
            }
            return true;
        };
        content_reader(
                [&](httplib::MultipartFormData const& file) {
                    return open(file.filename.empty() ? "" : _dataDir + file.filename);
                },
                [&](char const* data, size_t data_length) { return write(data, data_length); });
        close();
    });
    size_t chunksSent = 0;
    svr.Get("/stream", [&](auto const& req, auto& res) {
        size_t const CHUNK_SIZE = 1024 * 1024;
        chunksSent = 0;
        res.set_content_provider(
                data.size(), "text/plain",
                [&data, &chunksSent, CHUNK_SIZE](size_t offset, size_t length, httplib::DataSink& sink) {
                    cout << "Stream: " << offset << " " << length << " chunksSent: " << chunksSent << endl;
                    sink.write(&data[offset], min(length, CHUNK_SIZE));
                    chunksSent++;
                    return true;
                },
                [&](bool success) { cout << "Stream: " << (success ? "completed" : "failed") << endl; });
    });
    svr.Get("/stream_without_content_length", [&](auto const& req, auto& res) {
        size_t const NUM_CHUNKS = 4;
        chunksSent = 0;
        res.set_content_provider(
                "text/plain", [&data, &chunksSent, NUM_CHUNKS](size_t offset, httplib::DataSink& sink) {
                    if (chunksSent++ < NUM_CHUNKS) {
                        cout << "Stream: " << offset << " chunksSent: " << chunksSent << endl;
                        sink.write(data.data(), data.size());
                    } else {
                        cout << "Stream: completed" << endl;
                        sink.done();
                    }
                    return true;
                });
    });
    svr.set_error_handler([](auto const& req, auto& res) {
        string const err =
                "<p>Error Status: <span style='color:red;'>" + to_string(res.status) + "</span></p>";
        res.set_content(err, "text/html");
    });
    if (_verbose) {
        svr.set_logger([&](auto const& req, auto const& res) {
            logger(req, res, _verboseDumpRequestBody, _verboseDumpResponseBody);
        });
    }
    thread watcher([&]() {
        size_t prevCount = 0;
        while (true) {
            this_thread::sleep_for(chrono::milliseconds(_reportIntervalMs));
            if (!_progress) continue;
            size_t const currCount = count;
            cout << timestamp() << ": " << (currCount - prevCount) << " req/s" << endl;
            prevCount = currCount;
        }
    });
    watcher.detach();

    // Configure the thread pool and the depth of the request queue. Both parameters
    // are optional and have default values. Both are tied together in the API.
    _numThreads = _numThreads == 0 ? thread::hardware_concurrency() : _numThreads;
    svr.new_task_queue = [&] { return new httplib::ThreadPool(_numThreads, _maxQueuedRequests); };
    if (_verbose) {
        cout << timestamp() << ": thread pool size: " << _numThreads
             << ", max requests queue size: " << _maxQueuedRequests << endl;
    }
    if (_port == 0) {
        _port = svr.bind_to_any_port(_bindAddr, _port);
        if (_port < 0) {
            cerr << timestamp() << ": failed to bind the server to any port" << endl;
            return 1;
        }
    } else {
        if (!svr.bind_to_port(_bindAddr, _port)) {
            cerr << timestamp() << ": failed to bind the server to the port: " << _port << endl;
            return 1;
        }
    }
    if (_verbose) {
        cout << timestamp() << ": starting the server on " << _bindAddr << ":" << _port << endl;
    }
    if (!svr.listen_after_bind()) {
        cerr << timestamp() << ": failed to start the server" << endl;
        return 1;
    }
    return 0;
}

}  // namespace lsst::qserv::replica
