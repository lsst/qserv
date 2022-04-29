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
#ifndef LSST_QSERV_REPLICA_HTTPCLIENTAPP_H
#define LSST_QSERV_REPLICA_HTTPCLIENTAPP_H

// Qserv headers
#include "replica/Application.h"
#include "replica/HttpClient.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpClientApp implements a tool that sends requests to a Web server over
 * the HTTP/HTTPS protocol. If option '--out=<file>' is present the result
 * will be writted to the specified file. Otherwise the content will be printed to
 * the standard output stream.
 */
class HttpClientApp : public Application {
public:
    typedef std::shared_ptr<HttpClientApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv Ahe vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    HttpClientApp() = delete;
    HttpClientApp(HttpClientApp const&) = delete;
    HttpClientApp& operator=(HttpClientApp const&) = delete;

    virtual ~HttpClientApp() override = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see HttpClientApp::create()
    HttpClientApp(int argc, char* argv[]);

    std::string _method = "GET";
    std::string _url;
    std::string _data;
    std::string _header;
    HttpClientConfig _clientConfig;
    std::string _file;
    bool _result2json = false;
    bool _silent = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_HTTPCLIENTAPP_H */
