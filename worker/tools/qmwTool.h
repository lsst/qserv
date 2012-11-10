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
 *
 * Qserv Metadata Worker tool
 */
 
#ifndef LSST_QSERV_WORKER_QMWTOOL_H
#define LSST_QSERV_WORKER_QMWTOOL_H

#include "lsst/qserv/worker/Metadata.h"

class RunActions {
public:
    RunActions(bool verbose);
    void installMeta(string const&);
    void destroyMeta();
    void printMeta();
    void registerDb(string const&);
    void unregisterDb(string const&);
    void listDbs();
    void createExportPaths(string const&);
    void rebuildExportPaths(string const&);

private:
    void _validateDbName(string const&);
    void _validatePath(string const&);

private:
    lsst::qserv::worker::Metadata _m;
};

#endif /* LSST_QSERV_WORKER_QMWTOOL_H */

