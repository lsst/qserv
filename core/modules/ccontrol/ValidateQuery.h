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


#ifndef LSST_QSERV_CCONTROL_VALIDATEQUERY_H
#define LSST_QSERV_CCONTROL_VALIDATEQUERY_H


#include <memory>

#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/OrderByClause.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "sql/Schema.h"


namespace lsst {
namespace qserv {
namespace ccontrol {


/**
 * @brief Verify that the columns of a given query are present in a given schema. Provide a user-friendly
 * error message.
 *
 * It is understood that this is excuting against queries that derive from, but are not identical to, the
 * original user query, and on tables that may not be exactly the table the user is queriying against.
 * For example, this funciton may be used to validate the merge query, against the merge table, which the
 * user should know nothing about. The error messages are structured, and should continue to be developed,
 * so that the error message is helpful (as helpful as possible) to the user.
 *
 * This funciton is work-in-progres but ultimately should ensure that the query will run on a table with the
 * provided schema.
 *
 * @param inStmt The SELECT statement to verify.
 * @param schema The schema to verify the SELECT statment against.
 * @param errorReport The user-friendly error message string to populate.
 * @return true if query validation passed; there should be no errors running the query against the table.
 * @return false if query validation failed.
 */
bool validateQuery(std::shared_ptr<query::SelectStmt> const& inStmt,
                   sql::Schema const& schema,
                   std::string& errorReport) {
    auto verifyColumns = [](auto const& clause,
                            sql::Schema const& schema,
                            std::string& missingColumn) -> bool {
        query::ValueExprPtrVector usedValueExprs;
        clause.findValueExprs(usedValueExprs);
        for (auto& usedVE : usedValueExprs) {
            if (usedVE->isConstVal()) continue;
            auto matchedVE = std::find_if(schema.columns.begin(), schema.columns.end(),
                [&usedVE](auto const& column) { return usedVE->isSubsetOf(column); });
            if (matchedVE == schema.columns.end()) {
                if (usedVE->isColumnRef()) {
                    missingColumn = "'" + usedVE->getColumnRef()->getColumn() + "' ";
                }
                return false;
            }
        }
        return true;
    };

    // If we need to add the select list to this check: add `findValueExprs` function to get all the used
    // value exprs out of the select list; I suppose we want this to go down as far as the columns used by
    // any functions? (Maybe we need an argument to tell it to recurse until it finds VE's that are
    // columns...)

    std::string missingName;
    if (inStmt->hasOrderBy() &&
            not verifyColumns(inStmt->getOrderBy(), schema, missingName)) {
        errorReport = "Unknown column " + missingName + "in 'order clause'";
        return false;
    }
    if (inStmt->hasGroupBy() &&
            not verifyColumns(inStmt->getGroupBy(), schema, missingName)) {
        errorReport = "Unknown column " + missingName + "in 'group by clause'";
        return false;
    }
    if (inStmt->hasHaving() &&
            not verifyColumns(inStmt->getHaving(), schema, missingName)) {
        errorReport = "Unknown column " + missingName + "in 'having clause'";
        return false;
    }

    return true;
}


}}} // lsst::qserv::ccontrol


#endif // LSST_QSERV_CCONTROL_VALIDATEQUERY_H
