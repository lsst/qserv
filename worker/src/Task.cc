/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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

#include "lsst/qserv/worker/Task.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/TaskMsgDigest.h"
#include <boost/regex.hpp>

namespace qWorker = lsst::qserv::worker;

namespace {
    void updateSubchunks(std::string const& s, 
                         qWorker::Task::Fragment& f) {
        f.clear_subchunk(); // Empty out existing.
        std::stringstream ss;
        int sc;
        std::string firstLine = s.substr(0, s.find('\n'));
        int subChunkCount = 0;
        boost::regex re("\\d+");
        boost::sregex_iterator i;
        for(i = boost::make_regex_iterator(firstLine, re);
             i != boost::sregex_iterator(); ++i) {
            ss.str((*i).str(0));
            ss >> sc;
            f.add_subchunk(sc);
        }
    }
    
    void updateResultTables(std::string const& script, 
                            qWorker::Task::Fragment& f) {
        f.clear_resulttable();
        // Find resultTable prefix
        char const prefix[] = "-- RESULTTABLES:";
        int prefixLen = sizeof(prefix);
        std::string::size_type prefixOffset = script.find(prefix);
        if(prefixOffset == std::string::npos) { // no table indicator?
            return;
        }
        prefixOffset += prefixLen - 1; // prefixLen includes null-termination.
        std::string tables = script.substr(prefixOffset, 
                                           script.find('\n', prefixOffset)
                                       - prefixOffset);
        // actually, tables should only contain one table name.
        // FIXME: consider verifying this.
        f.set_resulttable(tables); 
    }
}

////////////////////////////////////////////////////////////////////////
// Task
////////////////////////////////////////////////////////////////////////
std::string const qWorker::Task::defaultUser = "qsmaster";

qWorker::Task::Task(qWorker::ScriptMeta const& s, std::string const& user_) {
    TaskMsgPtr t(new TaskMsg());
    hash = s.hash;
    dbName = s.dbName;
    resultPath = s.resultPath;
    msg->set_chunkid(s.chunkId);
    lsst::qserv::TaskMsg::Fragment* f = t->add_fragment();
    updateSubchunks(s.script, *f);
    updateResultTables(s.script, *f);
    f->set_query(s.script);
}

qWorker::Task::Task(qWorker::Task::TaskMsgPtr t, std::string const& user_) {
    hash = hashTaskMsg(*t);
    dbName = "q_" + hash;
    resultPath = hashToResultPath(hash);
    msg = t;
    user = user_;
}
