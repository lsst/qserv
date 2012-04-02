
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

using lsst::qserv::SqlConfig;
using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::string;
using std::stringstream;
using std::vector;

const char* execName = "qsDbTool";

int
printHelp() {
    cout
     << "\nNAME:\n"
     << "  " << execName << " [OPTION...] [ACTION] [ARGUMENTS]\n\n"
     << "DESCRIPTION:\n"
     << "  Manages qserv metadata\n\n"

     << "EXAMPLES:\n"
     << "  "<<execName<<" -a <mysqlAuth> -i <uniqueId> register "
                      << "<dbName> [<table1>] [<table2>] ...\n"
     << "  "<<execName<<" -a <mysqlAuth> -i <uniqueId> -b <baseDir> "
                      << "unregister <dbName>\n"
     << "  "<<execName<<" -a <mysqlAuth> -i <uniqueId> -b <baseDir> export "
                      <<"[<dbName>] [<dbName2>] ...\n"
     << "  "<<execName<<" -a <mysqlAuth> -i <uniqueId> show\n"
     << "  "<<execName<<" help\n\n"
     << "OPTIONS:\n"
     << "  -a <mysqlAuth>\n"
     << "  -i <uniqueId>\n"
     << "  -b <baseDir>\n"
     << "\nACTIONS:\n"
     << "  register\n"
     << "      registers database in qserv metadata\n\n"
     << "  unregister\n"
     << "      unregisters database from qserv metadata and destroys\n"
     << "      corresponding export structures for that database\n\n"
     << "  export\n"
     << "      generates export paths. If no dbName is given, it will\n"
     << "      run for all databases registered in qserv metadata.\n\n"
     << "  show\n"
     << "      prints qserv metadata\n\n"
     << "  help\n"
     << "      prints help screen and exits.\n"
     << "\nABOUT <uniqueId>:\n"
     << "  The uniqueId was introduced to allow running multiple masters\n"
     << "  and/or workers on the same machine. It uniquely identifies\n"
     << "  a master / a worker.\n"
     << "\nABOUT <mysqlAuth>:\n"
     << "  <mysqlAuth> should point to a config file. Format of one line \n"
     << "  of config file: <token>:<value>. (Parsing is very basic,\n"
     << "  so no extra spaces please.) Supported tokens: \n"
     << "  host, port, user, pass, sock. Example contents:\n"
     << "      host:localhost\n"
     << "      port:3306\n"
     << "      user:theMySqlUser\n"
     << "      pass:thePassword\n"
     << "      sock:/the/mysql/socket/file.sock\n"
     << endl;
    return 0;
}

int
printErrMsg(string const& msg, int err) {
    cout << execName << ": " << msg << "\n" 
         << "Try `" << execName << " -h` for more information.\n";
    return err;
}

// validates database and table names. Only a-z, A-Z, 0-9 and _ are allowed
bool
isValidName(string const& name, string const& x) {
    const string validChars = 
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    size_t found = name.find_first_not_of(validChars);
    if (found != string::npos) {
        cerr << "Invalid " << x << " name '" << name << "'. "
             << "Offending character: " << name[found] << endl;
        return false;
    }
    return true;
}

int
assembleSqlConfig(string const& authFile, SqlConfig& config) {
    //sc.socket = qWorker::getConfig().getString("mysqlSocket").c_str();
    ifstream f;
    f.open(authFile.c_str());
    if (!f) {
        stringstream s;
        s << "Failed to open '" << authFile << "'";
        return printErrMsg(s.str(), -100);
    }
    string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            stringstream s;
            s << "Invalid format, expecting <token>:<value>. "
              << "File '" << authFile << "', line: '" << line << "'";
            return printErrMsg(s.str(), -101);
        }
        string token = line.substr(0,pos);
        string value = line.substr(pos+1, line.size());
        if (token == "host") { 
            config.hostname = value;
        } else if (token == "port") {
            config.port = atoi(value.c_str());
            if ( config.port <= 0 ) {
                stringstream s;
                s << "Invalid port number " << config.port << ". "
                  << "File '" << authFile << "', line: '" << line << "'";
                return printErrMsg(s.str(), -102);
            }        
        } else if (token == "user") {
            config.username = value;
        } else if (token == "pass") {
            config.password = value;
        } else if (token == "sock") {
            config.socket = value;
        } else {
            stringstream s;
            s << "Unexpected token: '" << token << "'" 
              << " (supported tokens are: host, port, user, pass, sock)";
            return printErrMsg(s.str(), -103);
        }
        f >> line;
   }
   f.close();
   return 0;
}

bool
registerDb(SqlConfig& sc,
           string const& uniqueId,
           string const& dbName,
           string const& pTables) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(uniqueId);
    if ( !m.registerQservedDb(dbName, pTables, sqlConn, errObj) ) {
        cerr << "Failed to register db. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    cout << "Database " << dbName << " successfully registered." << endl;
    return 0;
}

bool
unregisterDb(SqlConfig& sc,
             string const& uniqueId,
             string const& dbName,
             string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(uniqueId);
    std::string dbPathToDestroy;
    if ( !m.unregisterQservedDb(dbName, baseDir, dbPathToDestroy,
                                sqlConn, errObj) ) {
        cerr << "Failed to unregister db. " 
             << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    lsst::qserv::worker::QservPathStructure::destroy(dbPathToDestroy);
    cout << "Database " << dbName << " successfully unregistered." << endl;
    return 0;
}

bool
showMetadata(SqlConfig& sc,
             string const& uniqueId) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(uniqueId);
    if ( !m.showMetadata(sqlConn, errObj) ) {
        cerr << "Failed to print metadata. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    return 0;
}

bool
generateExportPathsForDb(SqlConfig& sc,
                         string const& uniqueId,
                         string const& dbName, 
                         string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(uniqueId);
    
    vector<string> exportPaths;
    if ( !m.generateExportPathsForDb(baseDir, dbName,
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
    cout << "Export paths successfully created for db " 
         << dbName << "." << endl;
    return 0;
}

bool
generateExportPaths(SqlConfig& sc,
                    string const& uniqueId,
                    string const& baseDir) {
    lsst::qserv::SqlConnection sqlConn(sc);
    lsst::qserv::SqlErrorObject errObj;

    lsst::qserv::worker::Metadata m(uniqueId);
    
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
    cout << "Export paths successfully created for all " 
         << "databases registered in qserv metadata." << endl;
    return 0;
}

// parses arguments for all actions, and triggers the action
int
runAction(int argc, int curArgc, char* argv[], SqlConfig& sc, 
          string const& uniqueId, string const& baseDir) {
    string theAction = argv[curArgc];
    if ( !sc.isValid() ) {
        stringstream s;
        s << "-a <mysqlAuth> is required for action: '" << theAction << "'.";
        return printErrMsg(s.str(), -201);
    }
    if (uniqueId.empty()) {
        stringstream s;
        s << "-i <uniqueId> is required for action: '" << theAction << "'.";
        return printErrMsg(s.str(), -202);
    }
    if ((theAction == "register" || theAction == "show") && 
        !baseDir.empty() ) {
        stringstream s;
        s << "Option -b <baseDir> not needed for action '" << theAction << "'";
        return printErrMsg(s.str(), -203);
    }
    
    // get the dbName for "register and "unregister"
    string dbName;
    if (theAction == "register" || theAction == "unregister" ) {
        if (argc<curArgc+2) {
            stringstream s;
            s << "Argument(s) expected after action '"  << theAction << "'";
            return printErrMsg(s.str(), -204);
        }
        if ( ! isValidName(argv[curArgc+1], "database") ) {
            return -4;
        }
        dbName = argv[curArgc+1];
        curArgc += 2;
    }
    // no more arguments expected for "unregister" and "show"
    if (theAction == "unregister" || theAction == "show") {
        if ( curArgc<argc-1 ) {
            stringstream s;
            s << "Unexpected argument '" << argv[curArgc] << "' found.";
            return printErrMsg(s.str(), -205);
        }
    }
    // baseDir required for "export" and "unregister"
    if (theAction == "export" || theAction == "unregister") {
        if ( baseDir.empty() ) {
            stringstream s;
            s << "-b <baseDir> is required for action: '" << theAction << "'.";
            return printErrMsg(s.str(), -206);
        }
    }
    if (theAction == "register") {
        string pTables;
        for ( ; curArgc<argc; curArgc++) {
            if ( ! isValidName(argv[curArgc], "table") ) {
                return -5;
            }
            pTables += argv[curArgc];
            if (curArgc != argc-1) {
                pTables += ',';
            }
        }
        return registerDb(sc, uniqueId, dbName, pTables);
    } else if (theAction == "unregister") {
        return unregisterDb(sc, uniqueId, dbName, baseDir);
    } else if (theAction == "show") {
        return showMetadata(sc, uniqueId);
    } else if (theAction == "export") {
        if ( curArgc == argc-1 ) {
            return generateExportPaths(sc, uniqueId, baseDir);
        }
        for ( curArgc++ ; curArgc<argc ; curArgc++ ) {
            string dbName = argv[curArgc];
            if ( ! isValidName(dbName, "database") ) {
                return -5;
            }        
            generateExportPathsForDb(sc, uniqueId, dbName, baseDir);
        }
        return 0;
    }
    return -1;
}

int
main(int argc, char* argv[]) {
    const char* execName = "qsDbTool";//argv[0];
    if ( argc < 2 ) {
        return printHelp();
    }
    SqlConfig sc;
    string uniqueId, baseDir;
    int i;
    for (i=1 ; i<argc ; i++) {
        if (argv[i][0] == '-' && argv[i][1] == 'h') {
            return printHelp();
        }
    }
    for (i=1 ; i<argc ; i++) {
        string theArg = argv[i];
        if (theArg == "-a" || theArg == "-i" || theArg == "-b") {
            if (argc<i+2) {
                stringstream s;
                s << "Missing argument after " << theArg;
                return printErrMsg(s.str(), -301);
            }
            if (theArg == "-a") {
                int ret = assembleSqlConfig(argv[i+1], sc);
                if ( ret != 0 ) {
                    return ret;
                }
            } else if (theArg == "-i") {
                uniqueId = argv[i+1];
            } else if (theArg == "-b") {
                baseDir = argv[i+1];
            }
            i++;
        } else {
            if ( theArg != "register" &&
                 theArg != "unregister" &&
                 theArg != "show" &&
                 theArg != "export" ) {                    
                stringstream s;
                s << "Unrecognized action: '" << theArg << "'";
                return printErrMsg(s.str(), -302);
            }
            for (int k=i; k<argc ; k++) {
                if (argv[k][0] == '-') {
                    return printErrMsg("Unexpected argument order (hint: specify options first)", -303);
                }
            }
            return runAction(argc, i, argv, sc, uniqueId, baseDir);
        }
    }
    return printErrMsg("No action specified", -304);
}
