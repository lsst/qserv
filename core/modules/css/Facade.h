// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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
class KvInterfaceImplMem;

/** The class stores Qserv-specific metadata and state information from the
    Central State System.
 */
class Facade {
public:
virtual     ~Facade();

    // accessors
virtual bool containsDb(std::string const& dbName) const;
virtual     bool containsTable(std::string const& dbName,
                       std::string const& tableName) const;
virtual     bool tableIsChunked(std::string const& dbName,
                        std::string const& tableName) const;
virtual     bool tableIsSubChunked(std::string const& dbName,
                           std::string const& tableName) const;
virtual     bool isMatchTable(std::string const& dbName,
                      std::string const& tableName) const;
virtual     std::vector<std::string> getAllowedDbs() const;
virtual     std::vector<std::string> getChunkedTables(std::string const& dbName) const;
virtual     std::vector<std::string> getSubChunkedTables(std::string const& dbName) const;
virtual     std::vector<std::string> getPartitionCols(std::string const& dbName,
                                              std::string const& tableName) const;
virtual     int getChunkLevel(std::string const& dbName,
                      std::string const& tableName) const;
virtual     std::string getDirTable(std::string const& dbName,
                            std::string const& tableName) const;
virtual     std::string getDirColName(std::string const& dbName,
                              std::string const& tableName) const;
virtual     std::vector<std::string> getSecIndexColNames(std::string const& dbName,
                                                 std::string const& tableName) const;
virtual     StripingParams getDbStriping(std::string const& dbName) const;
virtual     double getOverlap(std::string const& dbName) const;
virtual     MatchTableParams getMatchTableParams(std::string const& dbName,
                                         std::string const& tableName) const;

    /**
     *  Returns current compiled-in version number of CSS data structures.
     *  This is not normally useful for clients but can be used by various tests.
     */
    static int cssVersion();

private:
    explicit Facade(std::istream& mapStream);
    explicit Facade(boost::shared_ptr<KvInterface> kv);

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
    void _versionCheck() const;

    friend class FacadeFactory;

private:
    boost::shared_ptr<KvInterface> _kvI;
protected:
    Facade() {}
    std::string _prefix; // optional prefix, for isolating tests from production
};

class FacadeFactory {
public:
    static boost::shared_ptr<Facade> createMemFacade(std::string const& mapPath);
    static boost::shared_ptr<Facade> createMemFacade(std::istream& mapStream);
    static boost::shared_ptr<Facade> createCacheFacade(boost::shared_ptr<KvInterface> kv);
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_FACADE_H
