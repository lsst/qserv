
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
using std::string;
using std::vector;

void
printHelp(const char* execName) {
    cout
     << "\nUsage:\n"
     << "   " << execName << " -r -c <mysqlAuth> -d <dbName> -t <tables>\n"
     << "   " << execName << " -u -c <mysqlAuth> -d <dbName>\n"
     << "   " << execName << " -s -c <mysqlAuth>\n"
     << "   " << execName << " -g -c <mysqlAuth> -a -b <baseDir>\n"
     << "   " << execName << " -g -c <mysqlAuth> -d <dbName> -b <baseDir>\n"
     << "   " << execName << " -h\n"
     << "\nWhere:\n"
     << "  -r             - register database in qserv metadata\n"
     << "  -u             - unregister database from qserv metadata\n"
     << "  -s             - show qserv metadata\n"
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
assembleSqlConfig(string const& authFile) {

    SqlConfig sc;
    //sc.socket = qWorker::getConfig().getString("mysqlSocket").c_str();

    ifstream f;
    f.open(authFile.c_str());
    if (!f) {
        cerr << "Failed to open '" << authFile << "'" << endl;
        assert(f);
    }
    string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            cerr << "Invalid format, expecting <token>:<value>. "
                 << "File '" << authFile 
                 << "', line: '" << line << "'" << endl;
            assert(pos != -1);
        }
        string token = line.substr(0,pos);
        string value = line.substr(pos+1, line.size());
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
           string const& workerId,
           string const& dbName,
           string const& pTables) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(workerId);
    if ( !m.registerQservedDb(dbName, pTables, sqlConn, errObj) ) {
        cerr << "Failed to register db. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    cout << "Database " << dbName << " successfully registered." << endl;
    return 0;
}

bool
unregisterDb(SqlConfig& sc,
           string const& workerId,
           string const& dbName) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(workerId);
    if ( !m.unregisterQservedDb(dbName, sqlConn, errObj) ) {
        cerr << "Failed to unregister db. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    cout << "Database " << dbName << " successfully unregistered." << endl;
    return 0;
}

bool
showMetadata(SqlConfig& sc,
             string const& workerId) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(workerId);
    if ( !m.showMetadata(sqlConn, errObj) ) {
        cerr << "Failed to print metadata. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    return 0;
}

bool
generateExportPathsForDb(SqlConfig& sc,
                         string const& workerId,
                         string const& dbName, 
                         string const& pTables,
                         string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(workerId);
    
    vector<string> exportPaths;
    if ( ! m.generateExportPathsForDb(baseDir, dbName, pTables,
                                      sqlConn, errObj, exportPaths) ) {
        cerr << "Failed to generate export directories. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    lsst::qserv::worker::QservPathStructure p;
    if ( !p.insert(exportPaths) ) {
        cerr << "Failed to insert export paths. "
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    if ( !p.persist() ) {
        cerr << "Failed to persist export paths. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    return 0;
}

bool
generateExportPaths(SqlConfig& sc,
                    string const& workerId,
                    string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(workerId);
    
    vector<string> exportPaths;
    if ( ! m.generateExportPaths(baseDir, sqlConn, errObj, exportPaths) ) {
        cerr << "Failed to generate export directories. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    lsst::qserv::worker::QservPathStructure p;
    if ( !p.insert(exportPaths) ) {
        cerr << "Failed to insert export paths. "
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    if ( !p.persist() ) {
        cerr << "Failed to persist export paths. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    return 0;
}

int 
main(int argc, char* argv[]) {
    const char* execName = argv[0];

    // TODO: get this from xrootd
    string const workerId = "theId";

    string dbName;
    string pTables;
    string baseDir;
    string authFile;
    
    bool flag_regDb = false; // register database
    bool flag_unrDb = false; // unregister database
    bool flag_genEp = false; // generated export paths
    bool flag_showM = false; // show metadata
    bool flag_allDb = false;

    int c;
    opterr = 0;
    while ((c = getopt (argc, argv, "rusgac:d:t:b:h")) != -1) {
        switch (c) {
        case 'r': { flag_regDb = true; break;}
        case 'u': { flag_unrDb = true; break;}
        case 's': { flag_showM = true; break;}
        case 'g': { flag_genEp = true; break;}
        case 'a': { flag_allDb = true; break;}
        case 'c': { authFile = optarg; break;}
        case 'd': { dbName   = optarg; break;} 
        case 't': { pTables  = optarg; break;}
        case 'b': { baseDir  = optarg; break;}
        case 'h': { printHelp(execName); return 0;}
        case '?':
            if (optopt=='c'||optopt=='d'||optopt=='t'||optopt=='b') {
                cerr << "Option -" << char(optopt) << " requires an argument." << endl;
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
    } else if ( flag_unrDb ) {
        if ( dbName.empty() ) {
            cerr << "database name not specified "
                 << "(must use -d <dbName> with -u option)" << endl;
            return -6;
        }
        return unregisterDb(sc, workerId, dbName);
    } else if ( flag_showM ) {
        return showMetadata(sc, workerId);
    } else if ( flag_genEp ) {
        if ( baseDir.empty() ) {
            cerr << "base dir not specified "
                 << "(must use -b <baseDir> with -g option)" << endl;
            return -7;
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
            return -8;
        }
        return 0;
    }
    cerr << "No option specified. (hint: use -r or -u or -g or -s)" << endl;
    printHelp(execName);
    return 0;
}
