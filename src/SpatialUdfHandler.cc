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
#include "lsst/qserv/master/SpatialUdfHandler.h"
 
// Pkg
#include "lsst/qserv/master/parseTreeUtil.h"

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

// Internal helpers
namespace {
    class strToDoubleFunc {
    public:
        double operator()(std::string const& s) { 
            char const* start = s.c_str();
            char const* eptr;
            // Cast away const. strtod won't write any chars anyway.
            double d = std::strtod(start, const_cast<char**>(&eptr));
            if(s.size() != static_cast<std::string::size_type>(eptr-start)) {
                std::stringstream s;
                s << "Exception converting string to double ("
                  << s << ")";
                throw s.str();
            }
            return d;
        }
    };
    
    // Tokenize a string delimited by ',' and place it into a container, 
    // transforming it if desired.
    template <class Container, class T>
    Container& tokenizeInto(std::string const& s, 
                            Container& c) {
        std::string delimiter(",");
        std::string::size_type pos = 0;
        std::string::size_type lastPos = 0;
        T transform;
        lastPos = s.find_first_not_of(delimiter, 0);
        while(std::string::npos != lastPos) {
            pos = s.find_first_of(delimiter, lastPos);
            std::string token(s, lastPos, pos-lastPos);
            c.push_back(transform(token));
            if(std::string::npos == pos) {
                break;
            } else {
                lastPos = s.find_first_not_of(delimiter, pos);
            }
        }
        return c;
    }
} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler::FromWhereHandler
////////////////////////////////////////////////////////////////////////
class qMaster::SpatialUdfHandler::FromWhereHandler : public VoidOneRefFunc {
public:
    FromWhereHandler(qMaster::SpatialUdfHandler& suh) : _suh(suh) {}
    virtual ~FromWhereHandler() {}
    virtual void operator()(antlr::RefAST fw) {
        if(!_suh._getIsPatched()) {
            if(_suh.getASTFactory() && !_suh.getWhereIntruder().empty()) {
                std::string intruder = "WHERE " + _suh.getWhereIntruder();
                insertTextNodeAfter(_suh.getASTFactory(), intruder, 
                                getLastSibling(fw));
            }
        } else {
            // Already patched, don't do anything.
        }
        std::cout << "fromWhere: " << walkTreeString(fw) << std::endl;
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
        std::cout << "whereCond: " << walkTreeString(where) << std::endl;
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
        std::cout << "Finalizing qserv restrictor spec" << std::endl;
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

        std::string paramStrRaw = walkTreeString(params);
        std::string paramStr = paramStrRaw.substr(0, paramStrRaw.size() - 1);
        std::list<double> paramNums;
        tokenizeInto<std::list<double>, strToDoubleFunc>(paramStr, paramNums);

        std::cout << "Got new restrictor spec " 
                  << name->getText() << "--";
        std::copy(paramNums.begin(), paramNums.end(), 
                  std::ostream_iterator<double>(std::cout, ","));
        std::cout << std::endl;
    }
private:
    qMaster::SpatialUdfHandler& _suh;
};

////////////////////////////////////////////////////////////////////////
// SpatialUdfHandler
////////////////////////////////////////////////////////////////////////
qMaster::SpatialUdfHandler::SpatialUdfHandler(antlr::ASTFactory* factory)
    : _fromWhere(new FromWhereHandler(*this)),
      _whereCond(new WhereCondHandler(*this)),
      _restrictor(new RestrictorHandler(*this)),
      _fctSpec(new FctSpecHandler(*this)),
      _isPatched(false),
      _factory(factory) {
    if(!_factory) {
        std::cerr << "WARNING: SpatialUdfHandler non-functional (null factory)"
                  << std::endl;
    }
    // For testing:
    //    double dummy[] = {0.0,0.0,1,1};
    //    setExpression("box",dummy, 4);
}

void qMaster::SpatialUdfHandler::setExpression(std::string const& funcName,
                                               double* first, int nitems) {
    std::stringstream ss;
    int oneless = nitems - 1;
    ss << funcName << "(";
    if(nitems > 0) {
        for(int i=0; i < oneless; ++i) {
            ss << first[i] << ", ";
        }
        ss << first[oneless];
    }
    ss << ")";
    _whereIntruder = ss.str();
}
    
