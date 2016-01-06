// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
 * @ingroup qana
 *
 * @brief Prevent execution of queries which have duplicated select fields
 * names.
 *
 * @author Fabrice Jammes, IN2P3/SLAC
 */

#ifndef LSST_QSERV_QANA_DUPLSELECTEXPRPLUGIN_H
#define LSST_QSERV_QANA_DUPLSELECTEXPRPLUGIN_H

// Parent class
#include "qana/QueryPlugin.h"

// System headers

// Third-party headers

// Qserv headers
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/typedefs.h"
#include "util/MultiError.h"

namespace lsst {
namespace qserv {
namespace qana {

namespace test {
    class DuplSelectExprPluginTestHelper;
}

/**
 * Prevent execution of queries which have duplicated select fields names.
 *
 * DuplSelectExprPlugin is the first plugin applied on logical query.
 * "Duplicated select field names" means that space-normalized and
 * lower-case fields name are equals.
 *
 */
class DuplSelectExprPlugin : public QueryPlugin {
    /**
      * Used to test private method
      */
    friend class test::DuplSelectExprPluginTestHelper;
public:

    /**
     * Template for exception message returned by Qserv
     */
    static std::string const EXCEPTION_MSG;

    /**
     * Template for error message created for each duplicated select field
     * found
     */
    static std::string const ERR_MSG;

    virtual ~DuplSelectExprPlugin() {}

    /**
     * Prevent execution of queries which have duplicated select fields names.
     *
     * @throw AnalysisError if duplicated select fields name are detected
     * @see QueryPlugin::applyLogical()
     */
    virtual void applyLogical(query::SelectStmt& stmt, query::QueryContext&);

private:

    /**
     * Returns duplicate select expressions names found in an SQL query
     *
     * @param stmt The select statement to analyze
     * @return a list of error. Each error contains a duplicate name, and its
     *         position in the select list.
     */
    util::MultiError getDuplicateSelectErrors(query::SelectStmt const& stmt) const;

    /**
     * Returns duplicate select expressions names found in a sequence of string
     *
     * @param v the sequence of string to analyze, must be space-normalized and
     *          lowercase
     * @return a list of error. Each error contains a duplicate field name, and its
     *         position in the select list
     */
    util::MultiError getDuplicateAndPosition(StringVector const& v) const;
};

}}} // namespace lsst::qserv::qana

#endif
