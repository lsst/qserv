// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_WORKER_QUERYSQL_BATCH_H
#define LSST_QSERV_WORKER_QUERYSQL_BATCH_H
 /**
  * @file QuerySql_Batch.h
  *
  * @brief QuerySql::Batch is the actual bundling portion of a QuerySql object.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "wdb/QuerySql.h"
namespace lsst { namespace qserv { namespace worker {

struct QuerySql::Batch {
    // Default to 10 SQL statements at a time.
    // Idea: Could add statements according to some cost metric(a
    // simple one) or to a certain overall query string length
    Batch(std::string const& name_,
          QuerySql::StringList const& sequence_, int batchSize_=10)
        : name(name_), batchSize(batchSize_),
          pos(0) {
        for(QuerySql::StringList::const_iterator i = sequence_.begin();
            i != sequence_.end(); ++i) {
            std::string::const_iterator last = i->begin() + (i->length() - 1);
            if(';' == *last) { // Clip trailing semicolon which
                // is added during batching.
                sequence.push_back(std::string(i->begin(), last));
            } else {
                sequence.push_back(*i);
            }
        }
    }
    bool isDone() const {
        return sequence.empty() || (pos >= sequence.size());
    }
    std::string current() const {
        std::ostringstream os;
        QuerySql::StringList::const_iterator begin;
        assert(pos < sequence.size()); // caller should have checked isDone()
        begin = sequence.begin() + pos;
        if(sequence.size() < (pos + batchSize)) {
            std::copy(begin, sequence.end(),
                      std::ostream_iterator<std::string>(os, ";\n"));
        } else {
            std::copy(begin, begin + batchSize,
                      std::ostream_iterator<std::string>(os, ";\n"));
        }
        return os.str();
    }
    void next() { pos += batchSize; }

    std::string name;
    QuerySql::StringList sequence;
    int batchSize;
    int pos;
};

}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_QUERYSQL_BATCH_H

