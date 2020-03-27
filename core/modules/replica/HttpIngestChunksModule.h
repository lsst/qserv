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
#ifndef LSST_QSERV_HTTPINGESTCHUNKSMODULE_H
#define LSST_QSERV_HTTPINGESTCHUNKSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/HttpModule.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpIngestChunksModule provides a support for registering new chunks
 * (or querying chunk disposition) in the Replication system as needed during
 * catalog ingest.
 */
class HttpIngestChunksModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestChunksModule> Ptr;

    static Ptr create(Controller::Ptr const& controller,
                      std::string const& taskName,
                      HttpProcessorConfig const& processorConfig);

    HttpIngestChunksModule() = delete;
    HttpIngestChunksModule(HttpIngestChunksModule const&) = delete;
    HttpIngestChunksModule& operator=(HttpIngestChunksModule const&) = delete;

    ~HttpIngestChunksModule() final = default;

protected:
    /**
     * Supported values for parameter 'subModuleName':
     *
     *   ADD-CHUNK      for registering (or requesting a status of) of a new chunk
     *   ADD-CHUNK-LIST for registering (or requesting a status of) of many new chunks
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    void executeImpl(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp,
                     std::string const& subModuleName) final;

private:
    HttpIngestChunksModule(Controller::Ptr const& controller,
                           std::string const& taskName,
                           HttpProcessorConfig const& processorConfig);

    /**
     * Register (if it's not register yet) a chunk for ingest.
     * Return connection parameters to an end-point service where chunk
     * data will need to be ingested.
     */
    void _addChunk(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Register (if it's not register yet) a list of chunks for ingest.
     * Return connection parameters to an end-point services (may differ
     * from chunk to chunk) where data of each chunk will need to be ingested.
     */
    void _addChunks(qhttp::Request::Ptr const& req,
                    qhttp::Response::Ptr const& resp);

    util::Mutex _ingestManagementMtx;   /// Synchronized access to the Ingest management operations
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPINGESTCHUNKSMODULE_H