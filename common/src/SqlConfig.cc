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
 */

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h> // atoi

#include "SqlConfig.hh"

using lsst::qserv::SqlConfig;

SqlConfig::SqlConfig(const SqlConfig& c) 
    : hostname(c.hostname),
      username(c.username),
      password(c.password),
      dbName(c.dbName),
      port(c.port),
      socket(c.socket) {
}

void 
SqlConfig::throwIfNotSet(std::string const& fName) const {
    bool allSet = true;
    std::stringstream s;
    s << "Value for ";
    if (hostname == "") { allSet = false; s << "host "; }
    if (port     == 0 ) { allSet = false; s << "port "; }
    if (username == "") { allSet = false; s << "username "; }
    if (password == "") { allSet = false; s << "password "; }
    if (dbName   == "") { allSet = false; s << "dbName ";   }
    if (socket   == "") { allSet = false; s << "socket "; }
    if (!allSet) {
        s << "not set in the '" << fName << "' file.";
        throw s.str();
    }
}

/// Initializes self from a file. File format: <key>:<value>
/// To ignore given token, pass "". 
/// To ignore unrecognized tokens, set the flag to false.
/// This is handy for reading a subset of a file. 
void 
SqlConfig::initFromFile(std::string const& fName,
                        std::string const& hostToken,
                        std::string const& portToken,
                        std::string const& userToken,
                        std::string const& passToken,
                        std::string const& dbNmToken,
                        std::string const& sockToken,
                        bool ignoreUnrecognizedTokens) {
    std::ifstream f;
    f.open(fName.c_str());
    if (!f) {
        std::stringstream s;
        s << "Failed to open '" << fName << "'";
        throw s.str();
    }
    std::string line;
    f >> line;
    while ( !f.eof() ) {
        int pos = line.find_first_of(':');
        if ( pos == -1 ) {
            std::stringstream s;
            s << "Invalid format, expecting <token>:<value>. "
              << "File '" << fName << "', line: '" << line << "'";
            throw s.str();
        }
        std::string token = line.substr(0,pos);
        std::string value = line.substr(pos+1, line.size());
        if (hostToken != "" and token == hostToken) { 
            this->hostname = value;
        } else if (portToken != "" and token == portToken) {
            this->port = atoi(value.c_str());
            if ( this->port <= 0 ) {
                std::stringstream s;
                s << "Invalid port number " << this->port << ". "
                  << "File '" << fName << "', line: '" << line << "'";
                throw s.str();
            }        
        } else if (userToken != "" and token == userToken) {
            this->username = value;
        } else if (passToken != "" and token == passToken) {
            this->password = value;
        } else if (dbNmToken != "" and token == dbNmToken) {
            this->dbName = value;
        } else if (sockToken != "" and token == sockToken) {
            this->socket = value;
        } else if (!ignoreUnrecognizedTokens) {
            std::stringstream s;
            s << "Unexpected token: '" << token << "' (supported tokens " 
              << "are: " << hostToken << ", " << portToken << ", "
              << userToken << ", " << passToken << ", " << dbNmToken << ", "
              << sockToken << ").";
            throw(s.str());
        }
        f >> line;
    }
    f.close();
    //throwIfNotSet(fName);
}   

void 
SqlConfig::printSelf(std::string const& extras) const {
    std::cout << "(" << extras << ") host=" << hostname << ", port=" << port 
              << ", usr=" << username << ", pass=" << password << ", dbName=" 
              << dbName << ", socket=" << socket << std::endl;
}
