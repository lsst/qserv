// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 LSST Corporation.
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

#ifndef LSST_QSERV_QUERY_QSRESTRICTOR_H
#define LSST_QSERV_QUERY_QSRESTRICTOR_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "global/stringTypes.h"

namespace lsst {
namespace qserv {
namespace query {

class QueryTemplate;

/// QsRestrictor is a Qserv spatial restrictor element that is used to
/// signal dependencies on spatially-partitioned tables. It includes
/// qserv-specific restrictors that make use of the spatial indexing,
/// but are not strictly spatial restrictions.
/// QsRestrictors can come from user-specification:
/// ... WHERE qserv_areaspec_box(1,1,2,2) ...
/// but may be auto-detected from predicates in the where clause.
/// ... WHERE objectId IN (1,2,3,4) ... --> qserv_objectid(1,2,3,4)
/// Some metadata checking is done in the process.
/// Names are generally one of:
/// qserv_fct_name :
///   "qserv_areaspec_box"^
///    | "qserv_areaspec_circle"^
///    | "qserv_areaspec_ellipse"^
///    | "qserv_areaspec_poly"^
///    | "qserv_areaspec_hull"^
/// but may include other names. They are used to pass information back
/// to the python layer to evaluate the geometry restriction.
class QsRestrictor {
public:
    typedef std::shared_ptr<QsRestrictor> Ptr;
    typedef std::vector<Ptr> PtrVector;

    class render {
    public:
        render(QueryTemplate& qt_) : qt(qt_) {}
        void operator()(std::shared_ptr<QsRestrictor> const& p);
        QueryTemplate& qt;
    };

    std::string _name;
    StringVector _params;
};
std::ostream& operator<<(std::ostream& os, QsRestrictor const& q);

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QSRESTRICTOR_H
