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
/**
 * SpatialUdfHandler - class for dealing with in-band spatial udf
 * specifiers.  Patches WHERE clauses appropriately with generated udf
 * function calls.
 */
#include "lsst/qserv/master/SpatialUdfHandler.h"
 
// Pkg
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/stringUtil.h"

// Boost
#include <boost/make_shared.hpp>
// std
#include <algorithm>
#include <cstdlib>
#include <list>
#include <iostream>
#include <iterator>
#include <sstream>

// namespace modifiers
namespace qMaster = lsst::qserv::master;

using std::stringstream;
using boost::make_shared;

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::Restriction
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::Restriction {
public:
    template <class C>
    Restriction(std::string const& name, C const& c) 
        : _name(name), _params(c.size()) { 
        std::copy(c.begin(), c.end(), _params.begin()); 
        _setGenerator();
    }
    Restriction(std::string const& name, double const* first, int nItems) 
        : _name(name), _params(nItems) { 
        std::copy(first, first+nItems, _params.begin());
        _setGenerator();
    }
    
    std::string getUdfCallString(std::string const& tName, 
                                 StringMap const& tableConfig) const {
        if(_generator.get()) {
            return (*_generator)(tName, tableConfig);
        }
        return std::string();
    }
    class Generator {
    public:
        virtual ~Generator() {}
        virtual std::string operator()(std::string const& tName, 
                                       StringMap const& tableConfig) = 0; 
    private:
    };
private:
    class ObjectIdGenerator : public Generator {
    public:
        ObjectIdGenerator(std::vector<double> const& paramNums_) 
            :  paramNums(paramNums_) {}


        virtual std::string operator()(std::string const& tName, 
                                       StringMap const& tableConfig) {
            std::stringstream s;
            std::string oidStr(getFromMap<StringMap>(tableConfig,
                                                     "objectIdCol", 
                                                     "objectId"));
            s << oidStr << " IN (";
            // coerce params to integer.
            std::for_each(paramNums.begin(), paramNums.end(), 
                          coercePrint<int>(s, ","));
            s << ")";
            return s.str();
        }
        std::vector<double> const& paramNums; 
    };

    class AreaGenerator : public Generator {
    public:
        AreaGenerator(char const* fName_, int paramCount_,
                      std::vector<double> const& params_) 
            :  fName(fName_), paramCount(paramCount_), params(params_) {}

        virtual std::string operator()(std::string const& tName, 
                                       StringMap const& tableConfig) {
            std::stringstream s;
            std::string raStr(getFromMap<StringMap>(tableConfig,
                                                         "raCol", 
                                                         "ra"));
            std::string declStr(getFromMap<StringMap>(tableConfig,
                                                         "declCol", 
                                                         "decl"));
            
            s << "(qserv_" << fName << "(" << tName << "." << raStr 
              << "," << tName << "." << declStr << ",";
            if(paramCount == USE_STRING) {
                s << '"'; // Place params inside a string.
                std::for_each(params.begin(), params.end(), 
                              coercePrint<double>(s," "));
                s << '"';
            } else {
                std::for_each(params.begin(), params.end(), 
                              coercePrint<double>(s,","));
                if(params.size() > 
                   static_cast<std::vector<double>::size_type>(paramCount)) {
                    throw std::string("multi not supported yet");
                }
            }
            s << ") = 1)";
            return s.str();
        }
        char const* const fName;
        const int paramCount;
        std::vector<double> const& params; 
        static const int USE_STRING = -999;
    };
    void _setGenerator();
    std::string _name;
    std::vector<double> _params;
    boost::shared_ptr<Generator> _generator;
};

void qMaster::SpatialUdfHandler::Restriction::_setGenerator() {
    if(_name == "qserv_areaspec_box") {
        _generator.reset(dynamic_cast<Generator*>
                         (new AreaGenerator("ptInSphBox", 
                                            4, _params)));
    } else if(_name == "qserv_areaspec_circle") {
        _generator.reset(dynamic_cast<Generator*>
                         (new AreaGenerator("ptInSphCircle", 
                                            3, _params)));
    } else if(_name == "qserv_areaspec_ellipse") {
        _generator.reset(dynamic_cast<Generator*>
                         (new AreaGenerator("ptInSphEllipse", 
                                            5, _params)));
    } else if(_name == "qserv_areaspec_poly") {
        _generator.reset(dynamic_cast<Generator*>
                         (new AreaGenerator("ptInSphPoly", 
                                            AreaGenerator::USE_STRING,
                                            _params)));
    } else if(_name == "qserv_objectId") {
        ObjectIdGenerator* g = new ObjectIdGenerator(_params);
        _generator.reset(dynamic_cast<Generator*>(g));
    } else {
        std::cout << "Unmatched restriction spec: " << _name 
                  << ", ignoring." << std::endl;
    }
}

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::FromWhereHandler
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::FromWhereHandler : public VoidOneRefFunc {
public:
    FromWhereHandler(qMaster::SpatialUdfHandler& suh) : _suh(suh) {}
    virtual ~FromWhereHandler() {}
    virtual void operator()(antlr::RefAST fw) {
        if(!_suh._getIsPatched()) {
            _suh._finalizeOutBand();
            if(_suh.getASTFactory() && !_suh.getWhereIntruder().empty()) {
                std::string intruder = "WHERE " + _suh.getWhereIntruder();
                insertTextNodeAfter(_suh.getASTFactory(), intruder, 
                                getLastSibling(fw));
            }
        } else {
            // Already patched, don't do anything.
        }
        // std::cout << "fromWhere: " << walkTreeString(fw) << std::endl;
    }
private:
    qMaster::SpatialUdfHandler& _suh;
};

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::WhereCondHandler
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::WhereCondHandler : public VoidOneRefFunc {
public:
    WhereCondHandler(qMaster::SpatialUdfHandler& suh) : _suh(suh) {}
    virtual ~WhereCondHandler() {}
    virtual void operator()(antlr::RefAST where) {
        // If we see a where condition, we can immediately patch it.
        if(_suh.getASTFactory()  && !_suh.getWhereIntruder().empty()) {
            std::string intruder = _suh.getWhereIntruder() + " AND";
            insertTextNodeAfter(_suh.getASTFactory(), intruder, where);
        }
        // Remember that we patched the tree.
        _suh._markAsPatched();
        //std::cout << "whereCond: " << walkTreeString(where) << std::endl;
        //std::cout << "Got limit -> " << limit << std::endl;            
    }
private:
    qMaster::SpatialUdfHandler& _suh;
};

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::RestrictorHandler
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::RestrictorHandler : public VoidVoidFunc {
public:
    RestrictorHandler(qMaster::SpatialUdfHandler& suh) : _suh(suh) {}
    virtual ~RestrictorHandler() {}
    virtual void operator()() {
        
        //std::cout << "Finalizing qserv restrictor spec" << std::endl;
        _suh._setHasRestriction();
    }
private:
    qMaster::SpatialUdfHandler& _suh;
};

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::FctSpecHandler
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::FctSpecHandler : public VoidTwoRefFunc {
public:
    FctSpecHandler(qMaster::SpatialUdfHandler& suh) : _suh(suh) {}
    virtual ~FctSpecHandler() {}
    virtual void operator()(antlr::RefAST name, antlr::RefAST params) {
        std::stringstream ss;
        if(_suh._getHasRestriction()) {
            std::cout << "ERROR: conflicting restriction clauses."
                      << " Ignoring " << walkTreeString(name)
                      << std::endl;
            return;
        }
        std::string paramStrRaw = walkTreeString(params);
        std::string paramStr = paramStrRaw.substr(0, paramStrRaw.size() - 1);
        std::list<double> paramNums;

        tokenizeInto(paramStr, ",", paramNums, strToDoubleFunc());
        boost::shared_ptr<Restriction> r(new Restriction(name->getText(),
                                                         paramNums));
        _suh._restrictions.push_back(r);
        _suh._expandRestriction(*r, ss);
        // std::cout << "Spec yielded " 
        //           << ss.str() <<std::endl;
        // Edit the parse tree
        collapseNodeRange(name, getLastSibling(params));
        name->setText(ss.str());
    }
private:
    qMaster::SpatialUdfHandler& _suh;
};

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler
////////////////////////////////////////////////////////////////////////
qMaster::SpatialUdfHandler::SpatialUdfHandler(antlr::ASTFactory* factory, 
                                              StringMapMap const& tableConfigMap,
                                              StringPairList const& spatialTables)
    : _fromWhere(new FromWhereHandler(*this)),
      _whereCond(new WhereCondHandler(*this)),
      _restrictor(new RestrictorHandler(*this)),
      _fctSpec(new FctSpecHandler(*this)),
      _isPatched(false),
      _factory(factory),
      _hasRestriction(false),
      _tableConfigMap(tableConfigMap),
      _spatialTables(spatialTables) {
    if(!_factory) {
        std::cerr << "WARNING: SpatialUdfHandler non-functional (null factory)"
                  << std::endl;
    }

    _udfName["box"] = "qserv_ptInSphBox";
    _udfName["circle"] = "qserv_ptInSphCircle";
    _udfName["ellipse"] = "qserv_ptInSphEllipse";
    _udfName["poly"] = "ptInSphPoly";
    _specName["box"] = "qserv_areaspec_box";
    _specName["circle"] = "qserv_areaspec_circle";
    _specName["ellipse"] = "qserv_areaspec_ellipse";
    _specName["poly"] = "qserv_areaspec_poly";
    // For testing:
    //    double dummy[] = {0.0,0.0,1,1};
    //    setExpression("box",dummy, 4);
}

void qMaster::SpatialUdfHandler::addExpression(std::string const& funcName,
                                               double const* first, int nitems) {
    std::stringstream ss;
    boost::shared_ptr<Restriction> r(new Restriction(_specName[funcName], 
                                                     first, nitems));
    _restrictions.push_back(r);
    _hasProcessedOutBand = false;
}

qMaster::StringMap const& qMaster::SpatialUdfHandler::getTableConfig(std::string const& tName) const {
    StringMap sm;
    return getFromMap(_tableConfigMap, tName, sm);
}

class qMaster::SpatialUdfHandler::processWrapper {
public:
    processWrapper(SpatialUdfHandler& suh, std::ostream& o) 
        : _suh(suh), _o(o), _first(true) {}

    void operator()(boost::shared_ptr<Restriction> const& r) {
        if(!_first) _o << " AND ";
        _suh._expandRestriction(*r, _o);
        _first = false;
    }
    SpatialUdfHandler& _suh;
    std::ostream& _o;
    bool _first;
};


std::ostream& qMaster::SpatialUdfHandler::_expandRestriction(Restriction const& r, std::ostream& o) {
    // Debug printout:
    // std::cout << "Got new restrictor spec " 
    //    << name->getText() << "--";
    // std::copy(paramNums.begin(), paramNums.end(), 
    //           std::ostream_iterator<double>(std::cout, ","));
    StringPairList const& sp = getSpatialTables();
    StringPairList::const_iterator spi;
    StringPairList::const_iterator spe = sp.end();
    bool first = true;
    for(spi = sp.begin(); spi != spe; ++spi) {
        if(!first) o << " AND ";
        else first = false;
        //std::cout << spi->first << "------" << spi->second << std::endl;
        o << r.getUdfCallString(spi->second, 
                                getTableConfig(spi->first));
    }
    return o;
}

void qMaster::SpatialUdfHandler::_finalizeOutBand() {
    std::stringstream o;
    if(!_hasProcessedOutBand) {
        std::for_each(_restrictions.begin(), 
                      _restrictions.end(), processWrapper(*this, o));
        _hasProcessedOutBand = true;
    }
    if(!_whereIntruder.empty()) {
        _whereIntruder += " AND "  + o.str();
    } else {
        _whereIntruder = o.str();
    }
}

#if 0
// MySQL UDF signatures.  see udf/MySqlSpatialUdf.c in qserv/worker
    double qserv_angSep (ra1, dec1, ra2, dec2);
    int qserv_ptInSphBox (ra, dec, ramin, decmin, ramax,decmax);
    int qserv_ptInSphCircle (ra, dec, racenter, deccenter, radius);
    int  qserv_ptInSphEllipse (ra, dec, racenter,deccenter, smaa,smia,ang);
    int qserv_ptInSphPoly (ra, dec, ra0, poly);
    // poly = string.  "ra0 dec0 ra1 dec1 ra2 dec2 ..." 
    // space separated ra/dec pairs.
// Functions return 1 if true, 0 if false, NULL on error
#endif
