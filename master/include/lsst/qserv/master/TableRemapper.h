// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// TableRemapper computes substitution mappings for table refs in query parser.
#ifndef LSST_QSERV_MASTER_TABLEREMAPPER_H
#define LSST_QSERV_MASTER_TABLEREMAPPER_H
#include "lsst/qserv/master/common.h"

namespace lsst {
namespace qserv {
namespace master {
class TableNamer; // Forward
class TableRefChecker;

/// Computes substitution maps using the TableNamer namespace and the
/// TableRefChecker for chunk/subchunk information. 
class TableRemapper {
public:
    TableRemapper(TableNamer const& tn, TableRefChecker const& checker, 
                  std::string const& delim);

    StringMap getMap(bool overlap=false);
    StringMap getPatchMap();
private:
    TableNamer const& _tableNamer;
    TableRefChecker const& _checker;
    std::string const _delim; // transitional. Shouldn't need in the future.

};
}}} // lsst::qserv::master
#endif // LSST_QSERV_MASTER_TABLEREMAPPER_H
