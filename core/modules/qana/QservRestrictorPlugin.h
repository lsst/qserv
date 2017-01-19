// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_QANA_QSERVRESTRICTORPLUGIN_H
#define LSST_QSERV_QANA_QSERVRESTRICTORPLUGIN_H

// Qserv headers
#include "global/stringTypes.h"
#include "qana/QueryPlugin.h"
#include "query/BoolTerm.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace query {

class QsRestrictor;

}}} // namespace lsst::qserv::query



namespace lsst {
namespace qserv {
namespace qana {

struct RestrictorEntry {
    RestrictorEntry(std::string const& alias_,
                 StringPair const& chunkColumns_,
                 std::string const& secIndexColumn_)
        : alias(alias_),
          chunkColumns(chunkColumns_),
          secIndexColumn(secIndexColumn_)
        {}
    std::string alias;
    StringPair chunkColumns;
    std::string secIndexColumn;
};


/// QservRestrictorPlugin replaces a qserv restrictor spec with directives
/// that can be executed on a qserv mysqld. This plugin should be
/// execute after aliases for tables have been generates, so that the
/// new restrictor function clauses/phrases can use the aliases.
class QservRestrictorPlugin : public QueryPlugin {
public:
    // Types
    typedef std::shared_ptr<QservRestrictorPlugin> Ptr;
    class Restriction;

    virtual ~QservRestrictorPlugin() {}

    virtual void prepare() {}

    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&);
    virtual void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context);

private:
    query::BoolTerm::Ptr
        _makeCondition(std::shared_ptr<query::QsRestrictor> const restr,
                       RestrictorEntry const& restrictorEntry);
};


}}} // namespace lsst::qserv::qana

#endif /* LSST_QSERV_QANA_QSERVRESTRICTORPLUGIN_H */
