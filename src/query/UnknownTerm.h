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

#ifndef LSST_QSERV_QUERY_UNKNOWNTERM_H
#define LSST_QSERV_QUERY_UNKNOWNTERM_H

// Qserv headers
#include "query/BoolTerm.h"

namespace lsst::qserv::query {

/// UnknownTerm is a catch-all term intended to help the framework pass-through
/// syntax that is not analyzed, modified, or manipulated in Qserv.
class UnknownTerm : public BoolTerm {
public:
    typedef std::shared_ptr<UnknownTerm> Ptr;

    /// Write a human-readable version of this instance to the ostream for debug output.
    std::ostream& putStream(std::ostream& os) const override;

    /// Serialze this instance as SQL to the QueryTemplate.
    void renderTo(QueryTemplate& qt) const override;

    /// Make a deep copy of this term.
    std::shared_ptr<BoolTerm> clone() const override;

    bool operator==(const BoolTerm& rhs) const override;

protected:
    /// Serialize this instance to os for debug output.
    void dbgPrint(std::ostream& os) const override;
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_UNKNOWNTERM_H
