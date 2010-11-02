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
 
#include <boost/shared_ptr.hpp>
#include "lsst/qserv/master/parserBase.h"

namespace lsst {
namespace qserv {
namespace master {

class SpatialUdfHandler {
public:
    SpatialUdfHandler();
    boost::shared_ptr<VoidOneRefFunc> getFromWhereHandler() { 
        return _fromWhere; 
    }
    boost::shared_ptr<VoidOneRefFunc> getWhereCondHandler() {
        return _whereCond;
    }
    
private:
    void markAsPatched() { _isPatched = true; }
    bool getIsPatched() const { return _isPatched; }

    class FromWhereHandler;
    class WhereCondHandler;
    friend class FromWhereHandler;
    friend class WhereCondHandler;

    boost::shared_ptr<VoidOneRefFunc> _fromWhere;
    boost::shared_ptr<VoidOneRefFunc> _whereCond;
    bool _isPatched;
};

}}} // namespace lsst::qserv::master
