// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// SphericalBoxStrategy is a class designed to abstract the
// computations and constructs that are specific to the spherical-box
// style of partitioning. Ideally, we would have an abstract strategy
// class as an interface that all partitioning strategies would share,
// but this is difficult to do without at least one other strategy and
// we don't have the resources (yet) to implement another strategy.

#ifndef LSST_QSERV_MASTER_SPHERICALBOXSTRATEGY_H
#define LSST_QSERV_MASTER_SPHERICALBOXSTRATEGY_H
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
class FromList;
class QueryContext;
class QueryMapping;

class SphericalBoxStrategy {
public:
    SphericalBoxStrategy(FromList const& f, 
                         QueryContext& context);
    boost::shared_ptr<QueryMapping> getMapping();
    void patchFromList(FromList& f);

    static std::string makeSubChunkDbTemplate(std::string const& db); 
    // Make full overlap for now.
    static std::string makeOverlapTableTemplate(std::string const& table);
    static std::string makeChunkTableTemplate(std::string const& table);
    static std::string makeSubChunkTableTemplate(std::string const& table);

private:
    class Impl;
    void _import(FromList& f);

    QueryContext& _context;
    boost::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SPHERICALBOXSTRATEGY_H

