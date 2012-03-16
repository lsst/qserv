
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

#include <iostream>
#include <fstream>

namespace qWorker = lsst::qserv::worker;

using lsst::qserv::SqlConfig;
using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;

void
printHelp(const char* execName) {
    std::cout
        << "\nUsage:\n"
        << "   " << execName << " -r -c <mysqlAuth> -d <dbName> -t <tables>\n"
        << "   " << execName << " -g -c <mysqlAuth> -a -b <baseDir>\n"
        << "   " << execName << " -g -c <mysqlAuth> -d <dbName> -b <baseDir>\n"
        << "   " << execName << " -h\n"
        << "\nWhere:\n"
        << "  -r             - register database in qserv metadata\n"
        << "  -g             - generate export paths\n"
        << "  -c <mysqlAuth> - path to mysql auth file, see below for details\n"
        << "  -a             - for all databases registered in qserv metadata\n"
        << "  -d <dbName>    - database name\n"
        << "  -t <tables>    - comma-separated list of partitioned tables\n"
        << "  -b <baseDir>   - base directory\n"
        << "  -h             - prints help and exits\n"
        << "\n"
        << "Format of the mysqlAuthFile: <token>:<value>\n"
        << "Supported tokens: host, port, user, pass, sock\n"
        << "Example contents:\n"
        << "host:localhost\n"
        << "port:3306\n"
        << "user:theMySqlUser\n"
        << "pass:thePassword\n"
        << "sock:/the/mysql/socket/file.sock\n"
        << endl;
}

SqlConfig
assembleSqlConfig(std::string const& authFile) {

    SqlConfig sc;
    //sc.socket = qWorker::getConfig().getString("mysqlSocket").c_str();

    ifstream f;
    f.open(authFile.c_str());
    if (!f) {
        cerr << "Failed to open '" << authFile << "'" << endl;
        assert(f);
    }
    std::string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            cerr << "Invalid format, expecting <token>:<value>. "
                 << "File '" << authFile 
                 << "', line: '" << line << "'" << endl;
            assert(pos != -1);
        }
        std::string token = line.substr(0,pos);
        std::string value = line.substr(pos+1, line.size());
        if (token == "host") { 
            sc.hostname = value;
        } else if (token == "port") {
            sc.port = atoi(value.c_str());
            if ( sc.port <= 0 ) {
                cerr << "Invalid port number " << sc.port << ". "
                 << "File '" << authFile 
                 << "', line: '" << line << "'" << endl;
                assert (sc.port != 0);
            }        
        } else if (token == "user") {
            sc.username = value;
        } else if (token == "pass") {
            sc.password = value;
        } else if (token == "sock") {
            sc.socket = value;
        } else {
            cerr << "Unexpected token: '" << value << "'" << endl;
            assert(0);
        }
        f >> line;
   }
   f.close();
   return sc;
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

    std::string dbName;
    std::string pTables;
    std::string baseDir;
    std::string authFile;
    
    bool flag_regDb = false;
    bool flag_genEp = false;
    bool flag_allDb = false;

    int c;
    opterr = 0;
    while ((c = getopt (argc, argv, "rgac:d:t:b:h")) != -1) {
        switch (c) {
        case 'r': { flag_regDb = true; break;}
        case 'g': { flag_genEp = true; break;}
        case 'a': { flag_allDb = true; break;}
        case 'c': { authFile = optarg; break;}
        case 'd': { dbName   = optarg; break;} 
        case 't': { pTables  = optarg; break;}
        case 'b': { baseDir  = optarg; break;}
        case 'h': { printHelp(execName); return 0;}
        case '?':
            if (optopt=='c'||optopt=='d'||
                optopt=='t'||optopt=='b') {
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
    if ( authFile.empty() ) {
        cerr << "MySql authorization file not specified "
             << "(must use -c <mysqlAuth> option)" << endl;
        return -3;
    }
            
    SqlConfig sc = assembleSqlConfig(authFile);

    if ( flag_regDb ) {
        if ( dbName.empty() ) {
            cerr << "database name not specified "
                 << "(must use -d <dbName> with -r option)" << endl;
            return -4;
        } else if ( pTables.empty() ) {
            cerr << "partitioned tables not specified "
                 << "(must use -t <tables> with -r option)" << endl;
            return -5;
        }
        return registerDb(sc, workerId, dbName, pTables);        
    } else if ( flag_genEp ) {
        if ( baseDir.empty() ) {
            cerr << "base dir not specified "
                 << "(must use -b <baseDir> with -g option)" << endl;
            return -6;
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
            return -7;
        }
        return 0;
    }
    cerr << "No option specified. (hint: use -r or -g)" << endl;
    printHelp(execName);
    return 0;
}
