// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// PlanWriter is a class that writes SelectPlan objects from SelectStmt
// objects.

#ifndef LSST_QSERV_MASTER_PLANWRITER_H
#define LSST_QSERV_MASTER_PLANWRITER_H
#include "lsst/qserv/master/transaction.h"
#include "lsst/qserv/master/ChunkSpec.h"
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
class SelectPlan;
class SelectStmt;

class PlanWriter {
public:
    PlanWriter() {}

    boost::shared_ptr<SelectPlan> write(SelectStmt const& ss, 
                                        ChunkSpecList const& specs);
private:

};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_PLANWRITER_H

