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
#include "lsst/qserv/master/TableNamer.h"

// Boost
#include <boost/make_shared.hpp>
// std
#include <algorithm>
#include <cstdlib>
#include <deque>
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
    template <typename StrContainer>
    Restriction(std::string const& specName, StrContainer const& nameAndParams) 
        : _name(specName) {
        _setGenerator(nameAndParams);
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
        template <typename Iter>
        ObjectIdGenerator(Iter begin, Iter end) 
            : paramStrs(begin, end) {
        }

        virtual std::string operator()(std::string const& tName, 
                                       StringMap const& tableConfig) {
            std::stringstream s;
            std::string oidStr(getFromMap<StringMap>(tableConfig,
                                                     "objectIdCol", 
                                                     "objectId"));
            s << oidStr << " IN (";
            // coerce params to integer.
            std::for_each(paramStrs.begin(), paramStrs.end(), 
                          coercePrint<std::string>(s, ","));
            s << ")";
            return s.str();
        }
        std::vector<std::string> paramStrs;
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
            
            s << "(scisql_" << fName << "(" << tName << "." << raStr 
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

    template<typename StrContainer>
    void _setGenerator(StrContainer const& v) {
        if(_name == "qserv_areaspec_box") {
            _importParams(v.begin() + 1, v.end(), v.size()-1);
            _generator.reset(dynamic_cast<Generator*>
                             (new AreaGenerator("s2PtInBox", 
                                                4, _params)));
        } else if(_name == "qserv_areaspec_circle") {
            _importParams(v.begin() + 1, v.end(), v.size()-1);
            _generator.reset(dynamic_cast<Generator*>
                             (new AreaGenerator("s2PtInCircle", 
                                                3, _params)));
        } else if(_name == "qserv_areaspec_ellipse") {
            _importParams(v.begin() + 1, v.end(), v.size()-1);
            _generator.reset(dynamic_cast<Generator*>
                             (new AreaGenerator("s2PtInEllipse", 
                                                5, _params)));
        } else if(_name == "qserv_areaspec_poly") {
            _importParams(v.begin() + 1, v.end(), v.size()-1);
            _generator.reset(dynamic_cast<Generator*>
                             (new AreaGenerator("s2PtInPoly", 
                                                AreaGenerator::USE_STRING,
                                                _params)));
        } else if(_name == "qserv_objectId") {
            ObjectIdGenerator* g = new ObjectIdGenerator(v.begin() + 1, 
                                                         v.end());
            _generator.reset(dynamic_cast<Generator*>(g));
        } else {
            std::cout << "Unmatched restriction spec: " << _name 
                      << ", ignoring." << std::endl;
        }
    }

    template<typename Iter, typename Size>
    void _importParams(Iter begin, Iter end, Size size) {
        _params.resize(size);
        std::transform(begin, end, _params.begin(), strToDoubleFunc());
    }

    std::string _name;
    std::vector<double> _params;
    boost::shared_ptr<Generator> _generator;
};


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
                //std::cout << "patching via FromWhere" << std::endl;
                if(_suh._recentWhere.get()) {
                    std::string intruder = _suh.getWhereIntruder() + " AND ";
                    insertTextNodeAfter(_suh.getASTFactory(), intruder, 
                                        _suh._recentWhere);
                    _suh._recentWhere = 0; // Reset.
                } else {
                    std::string intruder = "WHERE " + _suh.getWhereIntruder();
                    insertTextNodeAfter(_suh.getASTFactory(), intruder, 
                                        getLastSibling(fw));
                }
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
        _suh._recentWhere = where;
        // If we see a where condition, we can immediately patch it.
        if(_suh.getASTFactory()  && !_suh.getWhereIntruder().empty()) {
            std::cout << "patching via Where" << std::endl;
            // std::cout << "Patching Where, orig=" 
            //           << where->getText() << std::endl;
            std::string intruder = _suh.getWhereIntruder() + " AND";
            insertTextNodeAfter(_suh.getASTFactory(), intruder, where);
            // Remember that we patched the tree.
            _suh._markAsPatched();
        }
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
        std::deque<std::string> paramStrs;
        tokenizeInto(paramStr, ",", paramStrs, passFunc<std::string>());
        paramStrs.push_front(name->getText());

        boost::shared_ptr<Restriction> r(new Restriction(name->getText(),
                                                         paramStrs));
        _suh._inbandRestrictions.push_back(r); // Track separately.
        _suh._expandRestriction(*r, ss);
        // std::cout << "Spec yielded " 
        //            << ss.str() <<std::endl;
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
qMaster::SpatialUdfHandler::SpatialUdfHandler(
                                           antlr::ASTFactory* factory, 
                                           StringMapMap const& tableConfigMap,
                                           qMaster::TableNamer const& tableNamer)
    : _fromWhere(new FromWhereHandler(*this)),
      _whereCond(new WhereCondHandler(*this)),
      _restrictor(new RestrictorHandler(*this)),
      _fctSpec(new FctSpecHandler(*this)),
      _isPatched(false),
      _factory(factory),
      _hasRestriction(false),
      _tableConfigMap(tableConfigMap),
      _tableNamer(tableNamer) {
    if(!_factory) {
        std::cerr << "WARNING: SpatialUdfHandler non-functional (null factory)"
                  << std::endl;
    }

    _udfName["box"] = "scisql_s2PtInBox";
    _udfName["circle"] = "scisql_s2PtInCircle";
    _udfName["ellipse"] = "scisql_s2PtInEllipse";
    _udfName["poly"] = "scisql_s2PtInCPoly";
    _specName["box"] = "qserv_areaspec_box";
    _specName["circle"] = "qserv_areaspec_circle";
    _specName["ellipse"] = "qserv_areaspec_ellipse";
    _specName["poly"] = "qserv_areaspec_poly";
    _specName["objectId"] = "qserv_objectId";
    // For testing:
    //    double dummy[] = {0.0,0.0,1,1};
    //    setExpression("box",dummy, 4);
}

void 
qMaster::SpatialUdfHandler::addExpression(std::vector<std::string> const& v) {
    boost::shared_ptr<Restriction> r(new Restriction(_specName[v[0]], v));
    //std::cout << "adding restriction: " << funcName << std::endl;
    _restrictions.push_back(r);
    _hasProcessedOutBand = false;
}

qMaster::StringMap const& 
qMaster::SpatialUdfHandler::getTableConfig(std::string const& tName) const {
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


std::ostream& 
qMaster::SpatialUdfHandler::_expandRestriction(Restriction const& r, 
                                               std::ostream& o) {
    // Debug printout:
    // std::cout << "Got new restrictor spec " 
    //    << name->getText() << "--";
    // std::copy(paramNums.begin(), paramNums.end(), 
    //           std::ostream_iterator<double>(std::cout, ","));
//first: alias
//second: phy
    typedef qMaster::TableNamer::RefDeque RefDeque;
    RefDeque const& rd = _tableNamer.getRefs();
    RefDeque::const_iterator rdi;
    RefDeque::const_iterator rde = rd.end();
    bool first = true;
    for(rdi = rd.begin(); rdi != rde; ++rdi) {
        if(!first) o << " AND ";
        else first = false;
        // std::cout << "Expanding restr for table: " 
        //           << spi->first << "--second--" 
        //           << spi->second << std::endl;
        std::string tname;
        if(rdi->isAlias) tname = rdi->logical;
        else tname = rdi->magic; 
        o << r.getUdfCallString(tname, 
                                getTableConfig(rdi->table));
    }
    return o;
}

void qMaster::SpatialUdfHandler::_finalizeOutBand() {
    std::stringstream o;
    if(!_hasProcessedOutBand) {
        //std::cout << "Processing out-band" << std::endl;
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
