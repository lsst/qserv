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

/// dispatcher.h - main interface to be exported via SWIG for the
/// frontend's Python layer to initiate subqueries and join them.
 
#ifndef LSST_QSERV_META_INITMETA_H
#define LSST_QSERV_META_INITMETA_H

namespace lsst {
namespace qserv {
namespace meta {

int addDbInfoNonPartitioned(int metaInfoId, char* dbName);

int addDbInfoPartitioned(int metaInfoId, char* dbName, 
                         int nStripes,
                         int nSubStripes,
                         float defOverlapF,
                         float defOverlapNN);

 // plus a similar one for addTableInfo...

}}}

#endif // LSST_QSERV_META_INITMETA_H
