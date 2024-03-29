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
/**
 * @file
 *
 * @brief Declarations for TableRefN and subclasses SimpleTableN and JoinRefN
 *
 * @author Daniel L. Wang, SLAC
 */

#ifndef LSST_QSERV_QUERY_JOINSPEC_H
#define LSST_QSERV_QUERY_JOINSPEC_H

// System headers
#include <iostream>
#include <memory>

// Forward declarations
namespace lsst::qserv::query {
class QueryTemplate;
class BoolTerm;
class ColumnRef;
}  // namespace lsst::qserv::query

namespace lsst::qserv::query {

/// JoinSpec is a parsed join spec.
/// join_spec :
///      join_condition
///       | named_columns_join
/// ;
/// join_condition :
///    "on" search_condition
/// ;
/// named_columns_join :
///    "using" LEFT_PAREN column_name_list/*join_column_list*/ RIGHT_PAREN
/// ;
/// search_condition :
///    boolean_term (boolean_term_op boolean_term)*
/// ;
/// search_condition is used for WHERE conditions as well.
class JoinSpec {
public:
    typedef std::shared_ptr<JoinSpec> Ptr;

    JoinSpec(std::shared_ptr<BoolTerm> const& onTerm) : _onTerm(onTerm) {}

    /// FIXME: not supporting join by multiple columns now
    JoinSpec(std::shared_ptr<ColumnRef> const& ref) : _usingColumn(ref) {}

    JoinSpec(std::shared_ptr<ColumnRef> ref, std::shared_ptr<BoolTerm> const& onTerm)
            : _usingColumn(ref), _onTerm(onTerm) {}

    std::shared_ptr<ColumnRef> getUsing() { return _usingColumn; }
    std::shared_ptr<ColumnRef const> getUsing() const { return _usingColumn; }
    std::shared_ptr<BoolTerm> getOn() { return _onTerm; }
    std::shared_ptr<BoolTerm const> getOn() const { return _onTerm; }

    std::ostream& putStream(std::ostream& os) const;
    void putTemplate(QueryTemplate& qt) const;
    Ptr clone() const;

    bool operator==(const JoinSpec& rhs) const;

    friend std::ostream& operator<<(std::ostream& os, JoinSpec const& js);
    friend std::ostream& operator<<(std::ostream& os, JoinSpec const* js);

private:
    std::shared_ptr<ColumnRef> _usingColumn;
    std::shared_ptr<BoolTerm> _onTerm;
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_JOINSPEC_H
