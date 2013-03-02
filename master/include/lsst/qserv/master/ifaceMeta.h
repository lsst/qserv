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

#ifndef LSST_QSERV_META_IFACEMETA_H
#define LSST_QSERV_META_IFACEMETA_H

// interface to metadata used by swig

namespace lsst {
namespace qserv {
namespace master {

int newMetadataSession();
void resetMetadataSession(int);
int addDbInfoNonPartitioned(int, char*);
int addDbInfoPartitionedSphBox(int, char*, int, int, float, float);
int addTbInfoNonPartitioned(int, char*, char*);
int addTbInfoPartitionedSphBox(int, char*, char*, float, char*, char*,
                               int, int, int, int);
void printMetadataCache(int);
void resetMetadataCache(int);

}}}

#endif // LSST_QSERV_META_IFACEMETA_H
