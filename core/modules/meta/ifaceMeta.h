/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file ifaceMeta.h
  *
  * @brief Interface used for exporting C++ metadata cache to python through swig.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_META_IFACEMETA_H
#define LSST_QSERV_META_IFACEMETA_H

#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {
class MetadataCache;

struct DbStriping {
    int stripes;
    int subStripes;
};

int newMetadataSession();
void discardMetadataSession(int);
int addDbInfoNonPartitioned(int, const char*);
int addDbInfoPartitionedSphBox(int, const char*, int, int, float, float);
int addTbInfoNonPartitioned(int, const char*, const char*);
int addTbInfoPartitionedSphBox(int, const char*, const char*, float,
                               const char*, const char*, const char*, int, int, int, int, int);
void printMetadataCache(int);

boost::shared_ptr<MetadataCache> getMetadataCache(int);
DbStriping getDbStriping(int metaSessionId, char const* dbName);
}}}

#endif // LSST_QSERV_META_IFACEMETA_H
