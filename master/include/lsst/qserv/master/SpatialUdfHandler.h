/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_SPATIALUDFHANDLER_H
#define LSST_QSERV_MASTER_SPATIALUDFHANDLER_H
#include <list> 
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/parserBase.h"
#include "lsst/qserv/master/common.h"
#include "antlr/ASTRefCount.hpp"

// Forward
namespace antlr {
class ASTFactory;
}

namespace lsst {
namespace qserv {
namespace master {
class TableNamer; // Forward

class SpatialUdfHandler {
public:
    typedef std::map<std::string,char const*> StringCharMap;

    /// @param factory : an ANTLR AST factory
    /// @param tableConfig : configuration of current spatial table.
    /// Only copies the reference, expecting the config to live
    /// (and probably change) over the life of this instance.
    SpatialUdfHandler(antlr::ASTFactory* factory, 
                      StringMapMap const& tableConfigMap,
                      TableNamer const& tableNamer);
    boost::shared_ptr<VoidOneRefFunc> getFromWhereHandler() { 
        return _fromWhere; 
    }
    boost::shared_ptr<VoidOneRefFunc> getWhereCondHandler() {
        return _whereCond;
    }
    boost::shared_ptr<VoidVoidFunc> getRestrictorHandler() {
        return _restrictor;
    }
    boost::shared_ptr<VoidTwoRefFunc> getFctSpecHandler() {
        return _fctSpec;
    }

    // @param vec is structured as [funcName, param1, param2, ...]
    void addExpression(std::vector<std::string> const& vec);
    
 private:
    // Where-clause manipulation
    class FromWhereHandler;
    class WhereCondHandler;
    friend class FromWhereHandler;
    friend class WhereCondHandler;
    // Restriction spec detection
    class RestrictorHandler;
    class FctSpecHandler;
    friend class RestrictorHandler;
    friend class FctSpecHandler;
    // Restriction spec
    class Restriction;
    typedef boost::shared_ptr<Restriction> RestrictionPtr;
    // Helper for restriction expansion
    class processWrapper;
    friend class processWrapper;

    void _markAsPatched() { _isPatched = true; }
    bool _getIsPatched() const { return _isPatched; }
    std::string getWhereIntruder() const { return _whereIntruder; }
    antlr::ASTFactory* getASTFactory() { return _factory; }
    void _setHasRestriction() { _hasRestriction = true; }
    bool _getHasRestriction() const { return _hasRestriction; } 
    StringMap const& getTableConfig(std::string const& tName) const;
    //StringPairList const& getSpatialTables() const { return _spatialTables; }
    
    void _finalizeOutBand();
    std::ostream& _expandRestriction(Restriction const& r, std::ostream& o);

    boost::shared_ptr<VoidOneRefFunc> _fromWhere;
    boost::shared_ptr<VoidOneRefFunc> _whereCond;
    boost::shared_ptr<VoidVoidFunc> _restrictor;
    boost::shared_ptr<VoidTwoRefFunc> _fctSpec;

    StringCharMap _udfName; // table lookup: consider making static.
    StringCharMap _specName; // table lookup: consider making static.

    bool _isPatched;
    antlr::ASTFactory* _factory;
    std::string _whereIntruder;
    std::list<RestrictionPtr> _restrictions;
    std::list<RestrictionPtr> _inbandRestrictions;
    bool _hasRestriction;
    bool _hasProcessedOutBand;
    StringMapMap const& _tableConfigMap;
    TableNamer const& _tableNamer;
    antlr::RefAST _recentWhere;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_SPATIALUDFHANDLER_H
