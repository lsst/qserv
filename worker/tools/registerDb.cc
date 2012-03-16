
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

#include <iostream>

namespace qWorker = lsst::qserv::worker;

using lsst::qserv::SqlConfig;
using std::cout;
using std::cerr;
using std::endl;

void
printHelp(const char* execName) {
    std::cout
        << "\nUsage:\n"
        << "   " << execName << " -r -d <dbName> -t <tables>\n"
        << "   " << execName << " -g -a -b <baseDir>\n"
        << "   " << execName << " -g -d <dbName> -b <baseDir>\n"
        << "   " << execName << " -h\n"
        << "\nWhere:\n"
        << "  -r           - register database in qserv metadata\n"
        << "  -g           - generate export paths\n"
        << "  -a           - for all databases registered in qserv metadata\n"
        << "  -d <dbName>  - database name\n"
        << "  -t <tables>  - comma-separated list of partitioned tables\n"
        << "  -b <baseDir> - base directory\n"
        << "  -h           - prints help and exits\n"
        << endl;
}

bool
registerDb(SqlConfig& sc,
           std::string const& workerId,
           std::string const& dbName,
           std::string const& pTables) {
    cout << "Registering db: " << dbName << ", partTables: " << pTables << endl;
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(workerId);
    if ( !m.registerQservedDb(dbName, pTables, sqlConn, errObj) ) {
        std::cerr << "Failed to register the db. " 
                  << errObj.printErrMsg() << std::endl;
        return errObj.errNo();
    }
    return 0;
}

bool
generateExportPathsForDb(SqlConfig& sc,
                         std::string const& workerId,
                         std::string const& dbName, 
                         std::string const& pTables,
                         std::string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(workerId);
    
    std::vector<std::string> exportPaths;
    if ( ! m.generateExportPathsForDb(baseDir, dbName, pTables,
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

bool
generateExportPaths(SqlConfig& sc,
                    std::string const& workerId,
                    std::string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(workerId);
    
    std::vector<std::string> exportPaths;
    if ( ! m.generateExportPaths(baseDir, sqlConn, errObj, exportPaths) ) {
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


int 
main(int argc, char* argv[]) {
    const char* execName = argv[0];

    // TODO: get this from xrootd
    std::string const workerId = "theId";

    // TODO: decide how to get these:
    SqlConfig sc;
    sc.hostname = "";
    sc.username = "adm_becla";
    sc.password = "";
    sc.dbName = "";
    sc.port = 0;
    sc.socket = qWorker::getConfig().getString("mysqlSocket").c_str();

    std::string dbName;
    std::string pTables;
    std::string baseDir;

    bool flag_regDb = false;
    bool flag_genEp = false;
    bool flag_allDb = false;

    int c;
    opterr = 0;
    while ((c = getopt (argc, argv, "rgd:t:ab:h")) != -1) {
        switch (c) {
        case 'r': { flag_regDb = true; break;}
        case 'g': { flag_genEp = true; break;}
        case 'a': { flag_allDb = true; break;}
        case 'd': { dbName  = optarg; break;} 
        case 't': { pTables = optarg; break;}
        case 'b': { baseDir = optarg; break;}
        case 'h': { printHelp(execName); return 0;}
        case '?':
            if (optopt=='r'||optopt=='d'||
                optopt=='t'||optopt== 'b') {
                cerr << "Option -" << optopt << " requires an argument." << endl;
            } else if (isprint (optopt)) {
                cerr << "Unknown option `-" << char(optopt) << "'" << endl;
            } else {
                cerr << "Unknown option character " << char(optopt) << endl;
            }
            return -1;
        default: {
            return -2;
        }
        }
    }
    if ( flag_regDb ) {
        if ( dbName.empty() ) {
            cerr << "database name not specified "
                 << "(must use -d <dbName> with -r option)" << endl;
            return -3;
        } else if ( pTables.empty() ) {
            cerr << "partitioned tables not specified "
                 << "(must use -t <tables> with -r option)" << endl;
            return -4;
        }
        return registerDb(sc, workerId, dbName, pTables);        
    } else if ( flag_genEp ) {
        if ( baseDir.empty() ) {
            cerr << "base dir not specified "
                 << "(must use -b <baseDir> with -g option)" << endl;
            return -5;
        }       
        if ( !dbName.empty() ) {
            cout << "Generating export paths for database: "
                 << dbName << ", baseDir is: " << baseDir << endl;
            return generateExportPathsForDb(sc, workerId, dbName, 
                                            pTables, baseDir);
        } else if ( flag_allDb ) {
            cout << "generating export paths for all databases "
                 << "registered in the qserv metadata, baseDir is: " 
                 << baseDir << endl;
            return generateExportPaths(sc, workerId, baseDir);
        } else {
            cerr << "\nDo you want to generate export paths for one "
                 << "database, or all? (hint: use -d <dbName or -a flag)" 
                 << endl;
            printHelp(execName);
            return -6;
        }
        // ...
        return 0;
    }
    printHelp(execName);
    return 0;
}
