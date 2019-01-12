// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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


#ifndef LSST_QSERV_QUERY_PASSTERM_H
#define LSST_QSERV_QUERY_PASSTERM_H


#include "query/BoolFactorTerm.h"
#include "query/LogicalTerm.h"


namespace lsst {
namespace qserv {
namespace query {


/// PassTerm is a catch-all boolean factor term that can be safely passed
/// without further analysis or manipulation.
class PassTerm : public BoolFactorTerm
{
  public: // text
    typedef std::shared_ptr<PassTerm> Ptr;

    PassTerm() {}
    PassTerm(const std::string& text) : _text(text) {}

    void findValueExprs(ValueExprPtrVector& vector) const override {}
    void findColumnRefs(ColumnRef::Vector& vector) const override {}

    virtual BoolFactorTerm::Ptr clone() const { return copySyntax(); }
    virtual BoolFactorTerm::Ptr copySyntax() const;
    virtual std::ostream& putStream(std::ostream& os) const;
    virtual void renderTo(QueryTemplate& qt) const;

    std::string _text;

    bool operator==(const BoolFactorTerm& rhs) const override;

protected:
    void dbgPrint(std::ostream& os) const override;
};


}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_PASSTERM_H
