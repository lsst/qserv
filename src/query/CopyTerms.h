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

#ifndef LSST_QSERV_QUERY_COPYTERMS_H
#define LSST_QSERV_QUERY_COPYTERMS_H

namespace lsst { namespace qserv { namespace query {

struct syntaxCopy {
    inline BoolTerm::Ptr operator()(BoolTerm::Ptr const& t) { return t ? t->copySyntax() : BoolTerm::Ptr(); }
    inline BoolFactorTerm::Ptr operator()(BoolFactorTerm::Ptr const& t) {
        return t ? t->copySyntax() : BoolFactorTerm::Ptr();
    }
};

struct deepCopy {
    inline BoolTerm::Ptr operator()(BoolTerm::Ptr const& t) { return t ? t->clone() : BoolTerm::Ptr(); }
    inline BoolFactorTerm::Ptr operator()(BoolFactorTerm::Ptr const& t) {
        return t ? t->clone() : BoolFactorTerm::Ptr();
    }
};

template <typename List, class Copy>
inline void copyTerms(List& dest, List const& src) {
    std::transform(src.begin(), src.end(), std::back_inserter(dest), Copy());
}

}}}  // namespace lsst::qserv::query

#endif  // LSST_QSERV_QUERY_COPYTERMS_H
