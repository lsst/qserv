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
#ifndef LSST_QSERV_MASTER_TABLESTRATEGY_H
#define LSST_QSERV_MASTER_TABLESTRATEGY_H
#include <list>
#include <boost/shared_ptr.hpp>
#include "query/TableRef.h"

namespace lsst {
namespace qserv {
namespace master {

class FromList;
class QueryContext;
class QueryMapping;

/// TableStrategy provides a structure for processing the FromList in
/// a way that facilitates the retention of the original structure
/// after processing.
class TableStrategy {
public:
    TableStrategy(FromList const& f,
                  QueryContext& context);
    boost::shared_ptr<QueryMapping> exportMapping();
    //void scan(FromList const& f);
    int getPermutationCount() const;
    boost::shared_ptr<TableRefList> getPermutation(int permutation, TableRefList const& tList);
    void setToPermutation(int permutation, TableRefList& p);

private:
    class Impl;
    void _import(FromList const& f);
    void _updateContext();

    boost::shared_ptr<Impl> _impl;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_SPHERICALBOXSTRATEGY_H

