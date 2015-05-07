// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
  * @file
  *
  * @brief Task is a bundle of query task fields
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "wbase/Task.h"

// Third-party headers
#include "boost/regex.hpp"
#include "boost/thread.hpp"

// Qserv headers
#include "lsst/log/Log.h"
#include "proto/TaskMsgDigest.h"
#include "proto/worker.pb.h"
#include "wbase/Base.h"


namespace {
    void updateSubchunks(std::string const& s,
                         lsst::qserv::wbase::Task::Fragment& f) {
        // deprecated though...
        f.mutable_subchunks()->clear_id();
        std::stringstream ss;
        int sc;
        std::string firstLine = s.substr(0, s.find('\n'));
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
                            lsst::qserv::wbase::Task::Fragment& f) {
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

    std::ostream&
    dump(std::ostream& os,
         lsst::qserv::proto::TaskMsg_Fragment const& f) {
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
} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wbase {

// Task::ChunkEqual functor
bool
Task::ChunkEqual::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if(!x || !y) { return false; }
    if((!x->msg) || (!y->msg)) { return false; }
    return x->msg->has_chunkid() && y->msg->has_chunkid()
        && x->msg->chunkid()  == y->msg->chunkid();
}
// Task::PtrChunkIdGreater functor
bool
Task::ChunkIdGreater::operator()(Task::Ptr const& x, Task::Ptr const& y) {
    if(!x || !y) { return false; }
    if((!x->msg) || (!y->msg)) { return false; }
    return x->msg->chunkid()  > y->msg->chunkid();
}

////////////////////////////////////////////////////////////////////////
// Task
////////////////////////////////////////////////////////////////////////
std::string const
Task::defaultUser = "qsmaster";

Task::Task(Task::TaskMsgPtr t, std::shared_ptr<wbase::SendChannel> sc) {
    // Make msg copy.
    msg = std::make_shared<proto::TaskMsg>(*t);
    sendChannel = sc;
    hash = hashTaskMsg(*t);
    dbName = "q_" + hash;
    if(t->has_user()) {
        user = t->user();
    } else {
        user = defaultUser;
    }
    timestr[0] = '\0';
    _poisoned = false;
}

void Task::poison() {
    std::shared_ptr<util::VoidCallable<void> > func;
    {
        boost::lock_guard<boost::mutex> lock(_mutex);
        if(_poisonFunc && !_poisoned) {
            func.swap(_poisonFunc);
        }
        _poisoned = true;
    }
    if(func) {
        (*func)();
    }
}

void Task::setPoison(std::shared_ptr<util::VoidCallable<void> > poisonFunc) {
    std::shared_ptr<util::VoidCallable<void> > func;
    {
        boost::lock_guard<boost::mutex> lock(_mutex);
        // Were we poisoned without a poison function available?
        if(_poisoned && !_poisonFunc) {
            func = _poisonFunc;
        } else {
            _poisonFunc = poisonFunc;
        }
    }
    if(func) {
        (*func)();
    } 
}

std::ostream& operator<<(std::ostream& os, Task const& t) {
    proto::TaskMsg& m = *t.msg;
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
}}} // namespace lsst::qserv::wbase
