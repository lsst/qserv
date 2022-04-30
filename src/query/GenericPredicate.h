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

#ifndef LSST_QSERV_QUERY_GENERICPREDICATE_H
#define LSST_QSERV_QUERY_GENERICPREDICATE_H

// Local headers
#include "query/Predicate.h"

namespace lsst::qserv::query {

/// GenericPredicate is a Predicate whose structure whose semantic meaning
/// is unimportant for qserv
class GenericPredicate : public Predicate {
public:
    typedef std::shared_ptr<GenericPredicate> Ptr;

    ~GenericPredicate() override = default;

    BoolFactorTerm::Ptr clone() const override;
    BoolFactorTerm::Ptr copySyntax() const override { return clone(); }
};

}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_GENERICPREDICATE_H
