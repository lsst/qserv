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


// forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class BoolTerm;
    class QsRestrictor;
    class QueryContext;
    class SelectStmt;
    class WhereClause;
}}} // end forward declarations


namespace lsst {
namespace qserv {
namespace qana {


class RestrictorEntry;


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

    void prepare() override {}

    void applyLogical(query::SelectStmt& stmt, query::QueryContext&) override;
    void applyPhysical(QueryPlugin::Plan& p, query::QueryContext& context) override;

    /// Return the name of the plugin class for logging.
    std::string name() const override { return "QservRestrictorPlugin"; }

private:
    std::shared_ptr<query::BoolTerm> _makeCondition(std::shared_ptr<const query::QsRestrictor> const restr,
                                                    RestrictorEntry const& restrictorEntry);

    /**
     * @brief Finds the qserv area restrictors in the where clause, adds the restrictor to the query context,
     *        and adds a scisql area restrictor to the where clause.
     *
     * Adding the qserv area restrictors to the context limits the chunks to which the query is sent.
     *
     * Adding the scisql function limits the searched area wihin the chunk(s).
     *
     * @param selectStmt The SELECT statement to process restrictors for.
     * @param context The query context to be updated.
     */
    void _handleQsRestrictors(query::SelectStmt& stmt, query::QueryContext& context);

    /**
     * @brief Looks in the WHERE clause for use of columns from chunked tables where chunk restrictions can
     *        be added, and adds qserv restrictor functions if any are found.
     *
     * @param whereClause The WHERE clause of the SELECT statement.
     * @param context The context to add the restrictor functions to.
     */
    void _handleSecondaryIndex(query::WhereClause& whereClause, query::QueryContext& context);
};


}}} // namespace lsst::qserv::qana


#endif /* LSST_QSERV_QANA_QSERVRESTRICTORPLUGIN_H */
