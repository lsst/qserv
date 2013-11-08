/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
 /**
  * @file Task.cc
  *
  * @brief Task is a bundle of query task fields
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include "lsst/qserv/worker/Task.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/TaskMsgDigest.h"
#include <boost/regex.hpp>

namespace qWorker = lsst::qserv::worker;

namespace {
    void updateSubchunks(std::string const& s, 
                         qWorker::Task::Fragment& f) {       
        // deprecated though...
        f.mutable_subchunks()->clear_id();
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
            f.mutable_subchunks()->add_id(sc);
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

    std::ostream& dump(std::ostream& os, 
                       lsst::qserv::TaskMsg_Fragment const& f) {
        os << "frag: " 
           << "q=";
        for(int i=0; i < f.query_size(); ++i) {
            os << f.query(i) << ",";        
        }
        if(f.has_subchunks()) {
            os << " sc=";
            for(int i=0; i < f.subchunks().id_size(); ++i) {
                os << f.subchunks().id(i) << ",";
            }
        }
        os << " rt=" << f.resulttable();
        return os;
    }
}

////////////////////////////////////////////////////////////////////////
// Task
////////////////////////////////////////////////////////////////////////
std::string const qWorker::Task::defaultUser = "qsmaster";

qWorker::Task::Task(qWorker::ScriptMeta const& s, std::string const& user_) {
    TaskMsgPtr t(new TaskMsg());
    // Try to force non copy-on-write
    hash.assign(s.hash.c_str());
    dbName.assign(s.dbName.c_str());
    resultPath.assign(s.resultPath.c_str());
    t->set_chunkid(s.chunkId);
    lsst::qserv::TaskMsg::Fragment* f = t->add_fragment();
    updateSubchunks(s.script, *f);
    updateResultTables(s.script, *f);
    f->add_query(s.script);
    needsCreate = false;
    msg = t;
    timestr[0] = '\0';
}

qWorker::Task::Task(qWorker::Task::TaskMsgPtr t, std::string const& user_) {
    hash = hashTaskMsg(*t);
    dbName = "q_" + hash;
    resultPath = hashToResultPath(hash);
    // Try to force non copy-on-write
    msg.reset(new TaskMsg(*t));
    user = user_;
    needsCreate = true;
    timestr[0] = '\0';
}

namespace lsst {
namespace qserv {
namespace worker {
std::ostream& operator<<(std::ostream& os, qWorker::Task const& t) {
    lsst::qserv::TaskMsg& m = *t.msg;
    os << "Task: " 
       << "msg: session=" << m.session() 
       << " chunk=" << m.chunkid()
       << " db=" << m.db() 
       << " entry time=" << t.timestr
       << " ";
    for(int i=0; i < m.fragment_size(); ++i) {
        dump(os, m.fragment(i));
        os << " ";
    }
    return os;
}
}}} // lsst::qserv::worker
