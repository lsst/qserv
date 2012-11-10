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
 * Qserv Metadata Worker tool. 
 * Parses arguments and does basic validation, fetches connection info
 * from ascii file, instructs Metadata object what to do and prints
 * success/failure status.
 */
 
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

#include "lsst/qserv/SqlConfig.hh"

using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::string;
using std::stringstream;
using std::vector;

using lsst::qserv::SqlConfig;

#include "qmwTool.h"

// ****************************************************************************
// ***** help
// ****************************************************************************
int
printHelp() {
    cout
     << "\nNAME\n"
     << "        qmwTool - program for managing qserv metadata on worker\n"
     << "\nSYNOPSIS\n"
     << "        qmwTool [-h|--help] [-v|--verbose] COMMAND [ARGS]\n"
     << "\nOPTIONS\n"
     << "   -h, --help\n"
     << "        Prints help information.\n"
     << "   -v, --verbose\n"
     << "        Turns on verbose mode.\n"
     << "\nCOMMANDS\n"
     << "  installMeta\n"
     << "        Sets up internal qserv metadata database.\n"
     << "        Arguments: <exportDir>\n\n"
     << "  destroyMeta\n"
     << "        Destroys internal qserv metadata database.\n\n"
     << "  printMeta\n"
     << "        Prints all metadata for given worker.\n\n"
     << "  registerDb\n"
     << "        Registers database for qserv use for given worker.\n"
     << "        Arguments: <dbName>\n\n"
     << "  unregisterDb\n"
     << "        Unregisters database used by qserv and destroys\n"
     << "        corresponding export structures for that database.\n"
     << "        Arguments: <dbName>\n\n"
     << "  listDbs\n"
     << "        List database names registered for qserv use.\n\n"
     << "  createExportPaths\n"
     << "        Generates export paths. If no dbName is given, it will\n"
     << "        run for all databases registered in qserv metadata\n"
     << "        for given worker. Arguments: [<dbName>]\n\n"
     << "  rebuildExportPaths\n"
     << "        Removes existing export paths and recreates them.\n"
     << "        If no dbName is given, it will run for all databases\n"
     << "        registered in qserv metadata for given worker.\n"
     << "        Arguments: [<dbName>]\n\n"
     << "EXAMPLES\n"
     << "Example contents of the (required) '~/.qmwadm' file:\n"
     << "qmsHost:localhost\n"
     << "qmsPort:7082\n"
     << "qmsUser:qms\n"
     << "qmsPass:qmsPass\n"
     << "qmsDb:testX\n"
     << "qmwUser:qmw\n"
     << "qmwPass:qmwPass\n"
     << "qmwMySqlSocket:/var/lib/mysql/mysql.sock\n"
     << endl;
    return 0;
}

// ****************************************************************************
// ***** processing actions
// ****************************************************************************
RunActions::RunActions(bool verboseMode) {
    string fName = getenv("HOME");
    fName += "/.qmwadm";
    SqlConfig sC, wC; // server and worker connection configs
    sC.initFromFile(fName, "qmsHost", "qmsPort", "qmsUser", "qmsPass",
                    "qmsDb", "", true);
    wC.initFromFile(fName, "", "", "qmwUser", "qmwPass", "", 
                    "qmwMySqlSocket", true);
    sC.dbName = "qms_" + sC.dbName;
    if (verboseMode) {
        sC.printSelf("qms");
        wC.printSelf("qmw");
    }
    if (!_m.init(sC, wC, verboseMode)) {
        throw _m.getLastError();
    }
}

void
RunActions::installMeta(string const& exportDir) {
    _validatePath(exportDir);
    if ( !_m.installMeta(exportDir) ) {
        throw _m.getLastError();
    }
    cout << "Metadata successfully installed." << endl;
}

void
RunActions::destroyMeta() {
    if ( !_m.destroyMeta() ) {
        throw _m.getLastError();
    }
    cout << "Metadata successfully destroyed." << endl;
}

void
RunActions::printMeta() {
    if ( !_m.printMeta() ) {
        throw _m.getLastError();
    }
}

void
RunActions::registerDb(string const& dbName) {
    _validateDbName(dbName);
    if ( !_m.registerQservedDb(dbName) ) {
        throw _m.getLastError();
    }
    cout << "Database " << dbName << " successfully registered." << endl;
}

void
RunActions::unregisterDb(string const& dbName) {
    _validateDbName(dbName);
    if ( !_m.unregisterQservedDb(dbName) ) {
        throw _m.getLastError();
    }
    cout << "Database " << dbName << " successfully unregistered." << endl;
}

void
RunActions::listDbs() {
    vector<string> dbs;
    if ( !_m.getDbList(dbs) ) {
        throw _m.getLastError();
    }
    cout << "Registered databases:\n";
    vector<string>::const_iterator itr;
    for ( itr=dbs.begin(); itr!=dbs.end(); ++itr) {
        cout << "  " << *itr << "\n";
    }
    cout << endl;
}

void
RunActions::createExportPaths(string const& dbName) {
    string where;
    if (dbName != "") {
        _validateDbName(dbName);
        where = "database "; where += dbName;
    } else {
        where = "all databases";
    }
    if (!_m.createExportPaths(dbName)) {
        throw _m.getLastError();
    }
    cout << "Export paths successfully created for " << where << "." << endl;
}

void
RunActions::rebuildExportPaths(string const& dbName) {
    string where;
    if (dbName != "") {
        _validateDbName(dbName);
        where = "database "; where += dbName;
    } else {
        where = "all databases";
    }
    if (!_m.rebuildExportPaths(dbName)) {
        throw _m.getLastError();
    }
    cout << "Export paths successfully rebuild for " << where << "." << endl;
}

// ****************************************************************************
// ***** basic validation of arguments
// ****************************************************************************
// validates database name. Only a-z, A-Z, 0-9 and '_' are allowed
void
RunActions::_validateDbName(string const& name) {
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
RunActions::_validatePath(string const& path) {
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

// ****************************************************************************
// ***** main
// ****************************************************************************
int
main(int argc, char* argv[]) {
    if ( argc < 2 ) {
        return printHelp();
    }
    bool verboseMode = false;
    int i;
    string h1("-h");
    string h2("--help");
    for (i=1 ; i<argc ; i++) {
        if (0 == string("-h").compare(argv[i]) || 
            0 == string("--help").compare(argv[i]) ) {
            return printHelp();
        }
        if (0 == string("-v").compare(argv[i]) || 
            0 == string("--verbose").compare(argv[i]) ) {
            verboseMode = true;
        }
    }
    try {
        RunActions actions(verboseMode);
        string theAction = argv[1];
        if (theAction == "installMeta") {
            if (argc != 3) {
                throw string("'installMeta' requires argument: <exportDir>");
            }
            actions.installMeta(argv[2]);
        } else if (theAction == "destroyMeta") {
            actions.destroyMeta();
        } else if (theAction == "destroyMeta") {
            actions.destroyMeta();
        } else if (theAction == "printMeta") {
            actions.printMeta();
        } else if (theAction == "registerDb") {
            if (argc != 3) {
                throw string("'registerDb' requires argument: <dbName>");
            }
            actions.registerDb(argv[2]);
        } else if (theAction == "unregisterDb") {
            if (argc != 3) {
                throw string("'unregisterDb' requires argument: <dbName>");
            }
            actions.unregisterDb(argv[2]);
        } else if (theAction == "listDbs") {
            actions.listDbs();
        } else if (theAction == "createExportPaths") {
            if (argc == 3) actions.createExportPaths(argv[2]);
            else           actions.createExportPaths("");
        } else if (theAction == "rebuildExportPaths") {
            if (argc == 3) actions.rebuildExportPaths(argv[2]);
            else           actions.rebuildExportPaths("");
        } else {
            stringstream s;
            s << "Unsupported command: '" << argv[1] << "'. " 
              << "See -h for details.";
            throw s.str();
        }
    } catch (std::string str) {
      cerr << str << endl;
      return -1;
    }
    return 0;
}
