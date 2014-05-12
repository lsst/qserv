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
#ifndef LSST_QSERV_QUERY_JOINREF_H
#define LSST_QSERV_QUERY_JOINREF_H


// System headers
#include <iostream>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "query/JoinSpec.h"
#include "query/TableRef.h"


namespace lsst {
namespace qserv {
namespace query {

class QueryTemplate; // Forward
class BoolTerm;
class ColumnRef;

/// JoinRef combines a join_spec with the target join table.
/// e.g., in FROM Alice a LEFT JOIN Bob b USING(fooColumn)
/// the corresponding JoinRef represents "LEFT JOIN Bob b USING(fooColumn)"
/// Note that the "USING(fooColumn)" is represented by a contained JoinSpec.
///
/// qualified_join :
///        ( "inner" | outer_join_type ("outer")? )? "join" table_ref join_spec
///        | "natural" ( "inner" | outer_join_type ("outer")? )? "join" table_ref
///        | "union" "join" table_ref
///
class JoinRef {
public:
    typedef boost::shared_ptr<JoinRef> Ptr;
    enum Type {DEFAULT, INNER, LEFT, RIGHT, FULL, CROSS, UNION};

    JoinRef(TableRef::Ptr right_,
            Type jt, bool isNatural_,
            boost::shared_ptr<JoinSpec> spec_)
        : _right(right_),
          _joinType(jt),
          _isNatural(isNatural_),
          _spec(spec_) {}

    bool isNatural() const { return _isNatural; }
    Type getJoinType() const { return _joinType; }

    TableRef::CPtr getRight() const { return _right; }
    TableRef::Ptr getRight() { return _right; }

    boost::shared_ptr<JoinSpec const> getSpec() const { return _spec; }
    boost::shared_ptr<JoinSpec> getSpec() { return _spec; }

    std::ostream& putStream(std::ostream& os) const;
    void putTemplate(QueryTemplate& qt) const;
    Ptr clone() const;
private:
    void _putJoinTemplate(QueryTemplate& qt) const;
    TableRef::Ptr _right;
    Type _joinType;
    bool _isNatural;
    boost::shared_ptr<JoinSpec> _spec;
};
std::ostream& operator<<(std::ostream& os, JoinRef const& js);
std::ostream& operator<<(std::ostream& os, JoinRef const* js);

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_JOINREF_H
