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
 
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h> // atoi

#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/worker/Config.h"
#include "lsst/qserv/worker/Metadata.h"
#include "lsst/qserv/worker/QservPathStructure.h"

using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::string;
using std::stringstream;

using lsst::qserv::SqlConfig;

#include "qmwTool.h"

// keep in schema: dbName, dbUuid, baseDir

int
printHelp() {
    cout
     << "\nNAME\n"
     << "        qmwTool - program for managing qserv metadata on worker\n"
     << "\nSYNOPSIS\n"
     << "        qmwTool [-h|--help] COMMAND [ARGS]\n"
     << "\nOPTIONS\n"
     << "   -h, --help\n"
     << "        Prints help information.\n"
     << "\nCOMMANDS\n"
     << "  registerDb\n"
     << "        Registers database for qserv use for given worker\n"
     << "        Arguments: <dbName> <baseDir>\n\n"
     << "  unregisterDb\n"
     << "        Unregisters database used by qserv and destroys\n"
     << "        corresponding export structures for that database.\n"
     << "        Arguments: <dbName>\n\n"
     << "  listDbs\n"
     << "        List database names registered for qserv use.\n\n"
     << "  createExportPaths\n"
     << "        Generates export paths. If no dbName is given, it will\n"
     << "        run for all databases registered in qserv metadata\n"
     << "        for the given worker. Arguments [<dbName>]\n\n"
     << "EXAMPLES\n"
     << "Example contents of the (required) .qmw.auth file:\n"
     << "qmsHost:lsst-db3.slac.stanford.edu\n"
     << "qmsPort:4040\n"
     << "mysqlSocket:/tmp/mysql.sock\n"
     << endl;
    return 0;
}

int
printErrMsg(string const& msg, int err) {
    cout << "qmwTool: " << msg << "\n" 
         << "Try `qmwTool -h` for more information.\n";
    return err;
}

RunActions::RunActions() 
{
    string fName = getenv("HOME");
    fName += "/.qmw.auth";
    _qmsConnCfg.initFromFile(fName, "qmsHost", "qmsPort", "qmsUser",
                             "qmsPass", "qmsDb", "", true);
    _qmwConnCfg.initFromFile(fName, "", "", "qmwUser", 
                             "qmwPass", "", "qmwMySqlSocket", true);
    _qmsConnCfg.dbName = "qms_" + _qmsConnCfg.dbName;
    _qmsConnCfg.printSelf("qms");
    _qmwConnCfg.printSelf("qmw");
}

int
RunActions::registerDb(string const& dbName, string const& baseDir) {
    cout << "registering db " << dbName << endl;
    lsst::qserv::SqlConnection sqlConn(_qmwConnCfg);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(_qmsConnCfg);
    if ( !m.registerQservedDb(dbName, baseDir, sqlConn, errObj) ) {
        cerr << "Failed to register db. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    cout << "Database " << dbName << " successfully registered." << endl;
    return 0;
}

int
RunActions::unregisterDb(string const& dbName) {
    cout << "unregistering db " << dbName << endl;
    lsst::qserv::SqlConnection sqlConn(_qmwConnCfg);
    lsst::qserv::SqlErrorObject errObj;
    lsst::qserv::worker::Metadata m(_qmsConnCfg);
    std::string dbPathToDestroy;
    if ( !m.unregisterQservedDb(dbName, dbPathToDestroy, sqlConn, errObj) ) {
        cerr << "Failed to unregister db. " << errObj.printErrMsg() << endl;
        return errObj.errNo();
    }
    lsst::qserv::worker::QservPathStructure::destroy(dbPathToDestroy);
    cout << "Database " << dbName << " successfully unregistered." << endl;
    return 0;
}

void
RunActions::listDbs() {
    cout << "listDbs" << endl;
}

void
RunActions::createExportPaths(string const& dbName, 
                              string const& baseDir) {
    cout << "createExportP " << dbName << ", " << baseDir << endl;
}

// validates database name. Only a-z, A-Z, 0-9 and '_' are allowed
void
validateDbName(string const& name) {
    const string validChars = 
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    size_t found = name.find_first_not_of(validChars);
    if (found != string::npos) {
        stringstream s;
        s << "Invalid <dbName>: '" << name << "'. "
          << "Offending character: '" << name[found] << "'";
        throw s.str();
    }
}

void
validatePath(string const& path) {
    const string validChars = 
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_/";
    size_t found = path.find_first_not_of(validChars);
    if (found != string::npos) {
        stringstream s;
        s << "Invalid path: '" << path << "'. "
          << "Offending character: '" << path[found] << "'";
        throw s.str();
    }
}

int
main(int argc, char* argv[]) {
    if ( argc < 2 ) {
        cout << "argc is " << argc << endl;
        return printHelp();
    }
    int i;
    string h1("-h");
    string h2("--help");
    for (i=1 ; i<argc ; i++) {
        if (0 == string("-h").compare(argv[i]) || 
            0 == string("--help").compare(argv[i]) ) {
            return printHelp();
        }
    }
    try {    
        RunActions actions;
        string theAction = argv[1];
        if (theAction == "registerDb") {
            if (argc != 4) {
                throw"'registerDb' requires two arguments: <dbName> <baseDir>";
            }
            validateDbName(argv[2]);
            validatePath(argv[3]);
            actions.registerDb(argv[2], argv[3]);
        } else if (theAction == "unregisterDb") {
            if (argc != 3) {
                throw "'unregisterDb' requires argument: <dbName>";
            }
            validateDbName(argv[2]);
            actions.unregisterDb(argv[2]);
        } else if (theAction == "listDbs") {
            actions.listDbs();
        } else if (theAction == "createExportPaths") {
            if (argc != 3) {
                stringstream s;
                s << "'createExportPaths' requires one argument: <dbName>";
                throw s.str();
            }
            validateDbName(argv[2]);
            actions.createExportPaths(argv[2], argv[3]);
        } else {
            stringstream s;
            s << "Unsupported command: '" << argv[1] << "'. " 
              << "See -h for details.";
            throw s.str();
        }
    } catch (std::string str) {
      cout << "Exception raised: " << str << '\n';
      return -1;
    }
    return 0;
}
