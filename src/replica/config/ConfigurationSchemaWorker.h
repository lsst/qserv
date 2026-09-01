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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONSCHEMAWORKER_H
#define LSST_QSERV_REPLICA_CONFIGURATIONSCHEMAWORKER_H

// Qserv headers
#include "replica/config/ConfigurationSchema.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * This class ConfigurationSchemaWorker is a specialization of ConfigurationSchema for
 * constructing JSON schemas of the Configuration service.
 */
class ConfigurationSchemaWorker : public ConfigurationSchema {
public:
    ConfigurationSchemaWorker();
    ConfigurationSchemaWorker(ConfigurationSchemaWorker const&) = default;
    ConfigurationSchemaWorker& operator=(ConfigurationSchemaWorker const&) = default;
    ~ConfigurationSchemaWorker() = default;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGURATIONSCHEMAWORKER_H
