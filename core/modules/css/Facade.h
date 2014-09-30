// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file
  *
  * @brief A facade to the Central State System used by all Qserv core modules.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_FACADE_H
#define LSST_QSERV_CSS_FACADE_H

// System headers
#include <iostream>
#include <string>
#include <vector>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "css/KvInterface.h"
#include "css/MatchTableParams.h"
#include "css/StripingParams.h"

namespace lsst {
namespace qserv {
namespace css {

class KvInterface; // forward declaration

/** The class stores Qserv-specific metadata and state information from the
    Central State System.
 */

class Facade {
public:
    ~Facade();

    // accessors
    bool containsDb(std::string const& dbName) const;
    bool containsTable(std::string const& dbName,
                       std::string const& tableName) const;
    bool tableIsChunked(std::string const& dbName,
                        std::string const& tableName) const;
    bool tableIsSubChunked(std::string const& dbName,
                           std::string const& tableName) const;
    bool isMatchTable(std::string const& dbName,
                      std::string const& tableName) const;
    std::vector<std::string> getAllowedDbs() const;
    std::vector<std::string> getChunkedTables(std::string const& dbName) const;
    std::vector<std::string> getSubChunkedTables(std::string const& dbName) const;
    std::vector<std::string> getPartitionCols(std::string const& dbName,
                                              std::string const& tableName) const;
    int getChunkLevel(std::string const& dbName,
                      std::string const& tableName) const;
    std::string getDirTable(std::string const& dbName,
                            std::string const& tableName) const;
    std::string getDirColName(std::string const& dbName,
                              std::string const& tableName) const;
    std::vector<std::string> getSecIndexColNames(std::string const& dbName,
                                                 std::string const& tableName) const;
    StripingParams getDbStriping(std::string const& dbName) const;
    double getOverlap(std::string const& dbName) const;
    MatchTableParams getMatchTableParams(std::string const& dbName,
                                         std::string const& tableName) const;
private:
    Facade(std::string const& connInfo, int timeout_msec);
    Facade(std::string const& connInfo, int timeout_msec, std::string const& prefix);
    Facade(std::istream& mapStream);

    void _throwIfNotDbExists(std::string const& dbName) const;
    void _throwIfNotTbExists(std::string const& dbName,
                             std::string const& tableName) const;
    void _throwIfNotDbTbExists(std::string const& dbName,
                               std::string const& tableName) const;
    bool _containsTable(std::string const& dbName,
                        std::string const& tableName) const;
    bool _tableIsChunked(std::string const& dbName,
                         std::string const& tableName) const;
    bool _tableIsSubChunked(std::string const& dbName,
                            std::string const& tableName) const;
    int _getIntValue(std::string const& key, int defaultValue) const;

    friend class FacadeFactory;

private:
    KvInterface* _kvI;
    std::string _prefix; // optional prefix, for isolating tests from production
};

class FacadeFactory {
public:
    static boost::shared_ptr<Facade> createZooFacade(std::string const& connInfo,
                                                     int timeout_msec);
    static boost::shared_ptr<Facade> createMemFacade(std::string const& mapPath);
    static boost::shared_ptr<Facade> createMemFacade(std::istream& mapStream);
    static boost::shared_ptr<Facade> createZooTestFacade(
                                                     std::string const& connInfo,
                                                     int timeout_msec,
                                                     std::string const& prefix);
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_FACADE_H
