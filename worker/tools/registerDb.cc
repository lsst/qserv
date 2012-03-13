
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

#include <iostream>

namespace qWorker = lsst::qserv::worker;

using lsst::qserv::SqlConfig;


int main(int, char**) {
// get these through program arguments
    std::string const workerId = "theId";
    std::string dbName = "rplante_PT1_2_u_pt12prod_im3000_qserv";
    std::string partitionedTables = "Object, Source, ForcedSource";
    std::string baseDir = "/u1/qserv/xrootd-run";
    
    SqlConfig sc;
    sc.hostname = "";
    sc.username = "adm_becla";
    sc.password = "";
    sc.dbName = "";
    sc.port = 0;
    sc.socket = qWorker::getConfig().getString("mysqlSocket").c_str();
    
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(workerId);
    
    if ( !m.registerQservedDb(dbName, partitionedTables, sqlConn, errObj) ) {
        std::cerr << "Failed to register the db. " 
                  << errObj.printErrMsg() << std::endl;
        return errObj.errNo();
    }
    std::vector<std::string> exportPaths;
    if ( ! m.generateExportPathsForDb(baseDir, dbName, partitionedTables,
                                      sqlConn, errObj, exportPaths) ) {
        std::cerr << "Failed to generate export directories. " 
                  << errObj.printErrMsg() << std::endl;
        return errObj.errNo();
    }
    lsst::qserv::worker::QservPathStructure p;
    if ( !p.insert(exportPaths) ) {
        std::cerr << "Failed to insert export paths. "
                  << errObj.printErrMsg() << std::endl;
        return errObj.errNo();
    }
    if ( !p.persist() ) {
        std::cerr << "Failed to persist export paths. " 
                  << errObj.printErrMsg() << std::endl;
        return errObj.errNo();
    }
    return 0;
}
