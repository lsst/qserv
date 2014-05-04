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
#ifndef LSST_QSERV_QANA_TABLESTRATEGY_H
#define LSST_QSERV_QANA_TABLESTRATEGY_H

// System headers
#include <list>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "query/TableRef.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class QueryContext;
    class FromList;
}
namespace qana {
    class QueryMapping;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qana {

/// TableStrategy provides a structure for processing the FromList in
/// a way that facilitates the retention of the original structure
/// after processing.
class TableStrategy {
public:
    TableStrategy(query::FromList const& f,
                  query::QueryContext& context);
    boost::shared_ptr<QueryMapping> exportMapping();
    //void scan(query::FromList const& f);
    int getPermutationCount() const;
    boost::shared_ptr<query::TableRefList> getPermutation(
                 int permutation, query::TableRefList const& tList);
    void setToPermutation(int permutation, query::TableRefList& p);

private:
    class Impl;
    void _import(query::FromList const& f);
    void _updateContext();

    boost::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_TABLESTRATEGY_H

