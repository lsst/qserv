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
#ifndef LSST_QSERV_REPLICA_HTTPFILEREADERAPP_H
#define LSST_QSERV_REPLICA_HTTPFILEREADERAPP_H

// Qserv headers
#include "replica/Application.h"
#include "replica/IngestConfigTypes.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpFileReaderApp implements a tool that reads files from an object store
 * over the HTTP/HTTPS protocol. If option '--out=<file>' is present the file's content
 * will be writted into the specified file. Otherwise the content will be printed to
 * the standard output stream.
 */
class HttpFileReaderApp: public Application {
public:
    typedef std::shared_ptr<HttpFileReaderApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv Ahe vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    HttpFileReaderApp()=delete;
    HttpFileReaderApp(HttpFileReaderApp const&)=delete;
    HttpFileReaderApp& operator=(HttpFileReaderApp const&)=delete;

    virtual ~HttpFileReaderApp() override=default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see HttpFileReaderApp::create()
    HttpFileReaderApp(int argc, char* argv[]);

    std::string _method = "GET";
    std::string _url;
    std::string _data;
    std::string _header;
    HttpFileReaderConfig _fileReaderConfig;
    std::string _file;
    bool _silent = false;
};

}}} // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_HTTPFILEREADERAPP_H */
