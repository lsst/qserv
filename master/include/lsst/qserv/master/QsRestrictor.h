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
#ifndef LSST_QSERV_MASTER_QSRESTRICTOR_H
#define LSST_QSERV_MASTER_QSRESTRICTOR_H
/**
  * @file QsRestrictor.h
  *
  * @author Daniel L. Wang, SLAC
  */
#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { 
namespace qserv { 
namespace master {
class QueryTemplate;

/// QsRestrictor is a Qserv spatial restrictor element that is used to
/// signal dependencies on spatially-partitioned tables. It includes
/// qserv-specific restrictors that make use of the spatial indexing,
/// but are not strictly spatial restrictuions.
class QsRestrictor {
public:
    typedef boost::shared_ptr<QsRestrictor> Ptr;
    typedef std::list<Ptr> List;
    typedef std::list<std::string> StringList;

    class render {
    public:
        render(QueryTemplate& qt_) : qt(qt_) {}
        void operator()(boost::shared_ptr<QsRestrictor> const& p);
        QueryTemplate& qt;
    };

    std::string _name;
    StringList _params;
};
std::ostream& operator<<(std::ostream& os, QsRestrictor const& q);
}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_QSRESTRICTOR_H

