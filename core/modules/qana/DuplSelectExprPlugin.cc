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

// Class header
#include "qana/DuplSelectExprPlugin.h"

// System headers
#include <memory>

// Third-party headers
#include "boost/algorithm/string/case_conv.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/AnalysisError.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/typedefs.h"
#include "util/Error.h"
#include "util/IterableFormatter.h"
#include "util/MultiError.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.DuplSelectExprPlugin");
}

namespace lsst {
namespace qserv {
namespace qana {

std::string const DuplSelectExprPlugin::EXCEPTION_MSG = "Duplicate names detected in select expression,"
        " rewrite SQL query using alias: ";

std::string const DuplSelectExprPlugin::ERR_MSG = "'%1%' at positions:%2%";

util::MultiError DuplSelectExprPlugin::getDuplicateAndPosition(StringVector const& v) const {

    typedef std::multimap<std::string, int> MultiMap;

    util::MultiError multiError;

    LOGS(_log, LOG_LVL_DEBUG, "Looking for duplicate fields in: " << util::printable(v));

    MultiMap mm;
    int pos;
    for (StringVector::const_iterator it = v.begin(), end = v.end(); it!=end; ++it) {
        pos = it - v.begin();
        mm.insert(std::pair<std::string, int>(*it,pos));
     }

    for (MultiMap::iterator it = mm.begin(), end = mm.end();
        it != end;
        it = mm.upper_bound(it->first))
    {
        std::string key = it->first;
        int nb_elem = mm.count(it->first);
        if (nb_elem>1) {
            std::ostringstream os;
            MultiMap::iterator subIt;
            for (subIt=mm.equal_range(key).first; subIt!=mm.equal_range(key).second; ++subIt)
                os << ' ' << (*subIt).second+1;

            boost::format err_msg = boost::format(ERR_MSG) % key % os.str();

            util::Error error(util::ErrorCode::DUPLICATE_SELECT_EXPR, err_msg.str());
            multiError.push_back(error);
        }
    }

    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
          std::string msg;
          if (!multiError.empty()) {
              msg = "Duplicate select fields found:\n" + multiError.toString();
          }
          else {
              msg = "No duplicate select field.";
          }
          LOGS(_log, LOG_LVL_DEBUG, msg);
    }
    return multiError;
}

util::MultiError
DuplSelectExprPlugin::getDuplicateSelectErrors(query::SelectStmt const& stmt) const {

    query::SelectList const& selectList = stmt.getSelectList();
    query::ValueExprPtrVector valueExprList = *(selectList.getValueExprList());

    if (LOG_CHECK_LVL(_log, LOG_LVL_DEBUG)) {
        std::ostringstream stream;
        selectList.dbgPrint(stream);
        LOGS(_log, LOG_LVL_DEBUG, "Input stmt:\n" << stream.str());
    }

    StringVector selectExprNormalizedNames;

    for (query::ValueExprPtrVectorConstIter viter = valueExprList.begin();
        viter != valueExprList.end();
        ++viter) {
        query::ValueExpr const& ve = *(*viter);
        if (ve.isStar()) {
            continue;
        }
        std::string name;
        std::string alias = ve.getAlias();
        if (!alias.empty()) {
            name = alias;
        } else if (ve.isColumnRef()) {
            name = ve.getColumnRef()->column;
        } else {
            name = ve.toString();
        }
        boost::algorithm::to_lower(name);
        selectExprNormalizedNames.push_back(name);
    }

    return getDuplicateAndPosition(selectExprNormalizedNames);
}

void DuplSelectExprPlugin::applyLogical(query::SelectStmt& stmt,
                                        query::QueryContext&) {

    util::MultiError const dupSelectErrors = getDuplicateSelectErrors(stmt);

    if (!dupSelectErrors.empty()) {
        std::string msg = DuplSelectExprPlugin::EXCEPTION_MSG + dupSelectErrors.toOneLineString();
        throw AnalysisError(msg);
    }
}

////////////////////////////////////////////////////////////////////////
// DuplSelectExprPluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class DuplSelectExprPluginFactory : public QueryPlugin::Factory {
public:
    // Types
    typedef std::shared_ptr<DuplSelectExprPluginFactory> Ptr;
    DuplSelectExprPluginFactory() {}
    virtual ~DuplSelectExprPluginFactory() {}

    virtual std::string getName() const { return "DuplicateSelectExpr"; }
    virtual QueryPlugin::Ptr newInstance() {
        return std::make_shared<DuplSelectExprPlugin>();
    }
};

////////////////////////////////////////////////////////////////////////
// registerTablePlugin implementation
////////////////////////////////////////////////////////////////////////
namespace {
struct registerPlugin {
    registerPlugin() {
        DuplSelectExprPluginFactory::Ptr f = std::make_shared<DuplSelectExprPluginFactory>();
        QueryPlugin::registerClass(f);
    }
};
// Static registration
registerPlugin registerDuplSelectExprPlugin;
} // annonymous namespace

}}} // namespace lsst::qserv::qana
