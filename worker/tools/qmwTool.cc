
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h> // atoi


using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::string;
using std::stringstream;

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
     << "        for the given worker. Arguments <baseDir> [<dbName>]\n\n"
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
    _c.initFromFile(fName);
    _c.printSelf();
}

void
RunActions::registerDb(string const& dbName) {
    cout << "registering db " << dbName << endl;
}

void
RunActions::unregisterDb(string const& dbName) {
    cout << "unregistering db " << dbName << endl;
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

void
RunActions::ConnInfo::printSelf() const {
    cout << "host=" << _qmsHost
         << ", port=" << _qmsPort
         << ", usr=" << _qmsUser
         << ", pass=" << _qmsPass
         << ", qmwDb=" << _qmwDb
         << ", socket=" << _mSocket << endl;
}

void
RunActions::ConnInfo::throwIfNotSet(string const& fName) const {
    bool allSet = true;
    stringstream s;
    s << "Value for ";
    if (this->_qmsHost == "") { allSet = false; s << "qmsHost "; }
    if (this->_qmsPort == 0 ) { allSet = false; s << "qmsPort "; }
    if (this->_qmsUser == "") { allSet = false; s << "qmsUser "; }
    if (this->_qmsPass == "") { allSet = false; s << "qmsPass "; }
    if (this->_qmwDb   == "") { allSet = false; s << "qmwDb ";   }
    if (this->_mSocket == "") { allSet = false; s << "mSocket "; }
    if (!allSet) {
        s << "not set in the '" << fName << "' file.";
        throw s.str();
    }
}

void
RunActions::ConnInfo::initFromFile(string const& fName) {
    ifstream f;
    f.open(fName.c_str());
    if (!f) {
        stringstream s;
        s << "Failed to open '" << fName << "'";
        throw s.str();
    }
    string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            stringstream s;
            s << "Invalid format, expecting <token>:<value>. "
              << "File '" << fName << "', line: '" << line << "'";
            throw s.str();
        }
        string token = line.substr(0,pos);
        string value = line.substr(pos+1, line.size());
        if (token == "qmsHost") { 
            this->_qmsHost = value;
        } else if (token == "qmsPort") {
            this->_qmsPort = atoi(value.c_str());
            if ( this->_qmsPort <= 0 ) {
                stringstream s;
                s << "Invalid port number " << this->_qmsPort << ". "
                  << "File '" << fName << "', line: '" << line << "'";
                throw s.str();
            }        
        } else if (token == "qmsUser") {
            this->_qmsUser = value;
        } else if (token == "qmsPass") {
            this->_qmsPass = value;
        } else if (token == "qmwDb") {
            this->_qmwDb = value;
        } else if (token == "mysqlSocket") {
            this->_mSocket = value;
        } else {
            stringstream s;
            s << "Unexpected token: '" << token << "'" << " (supported tokens "
              << "are: qmsHost, qmsPort, qmsUser, qmsPass, mysqlSocket)";
            throw(s.str());
        }
        f >> line;
   }
   f.close();
   throwIfNotSet(fName);
}


// validates database name. Only a-z, A-Z, 0-9 and _ are allowed
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
            if (argc != 3) {
                throw "'registerDb' requires argument: <dbName>";
            }
            validateDbName(argv[2]);
            actions.registerDb(argv[2]);
        } else if (theAction == "unregisterDb") {
            if (argc != 3) {
                throw "'unregisterDb' requires argument: <dbName>";
            }
            validateDbName(argv[2]);
            actions.unregisterDb(argv[2]);
        } else if (theAction == "listDbs") {
            actions.listDbs();
        } else if (theAction == "createExportPaths") {
            if (argc != 4) {
                stringstream s;
                s << "'createExportPaths' requires two arguments: "
                  << "<dbName> <basePath>";
                throw s.str();
            }
            validateDbName(argv[2]);
            validatePath(argv[3]);
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
