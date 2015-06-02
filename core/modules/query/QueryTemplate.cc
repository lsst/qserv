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
  * @brief Implementation of QueryTemplate, which is a object that can
  * be used to generate concrete queries from a template, given
  * certain parameters (e.g. chunk/subchunk).
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/QueryTemplate.h"

// System headers
#include <iostream>
#include <sstream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/sqltoken.h" // sqlShouldSeparate
#include "query/ColumnRef.h"
#include "query/TableRef.h"


namespace lsst {
namespace qserv {
namespace query {

struct SpacedOutput {
    SpacedOutput(std::ostream& os_, std::string sep_=" ")
        : os(os_), sep(sep_), count(0) {}
    void operator()(std::string const& s) {
        if(s.empty()) return;

        if(!last.empty() && sql::sqlShouldSeparate(last, *(last.end()-1), s[0]))  {
            os << sep;
        }
        os << s;
        last = s;
    }
    void operator()(std::shared_ptr<QueryTemplate::Entry> e) {
        if(!e) { throw std::invalid_argument("NULL QueryTemplate::Entry"); }
        //if(e->isDynamic()) { os << "(" << count << ")"; }
        (*this)(e->getValue());
        ++count;
    }

    std::ostream& os;
    std::string last;
    std::string sep;
    int count;
};

template <typename C>
std::string outputString(C& c) {
    std::stringstream ss;
    SpacedOutput so(ss, " ");
    std::for_each(c.begin(), c.end(), so);
    return ss.str();
}
struct MappingWrapper {
    MappingWrapper(QueryTemplate::EntryMapping const& em_,
                   QueryTemplate& qt_)
        : em(em_), qt(qt_) {}
    void operator()(std::shared_ptr<QueryTemplate::Entry> e) {
            qt.append(em.mapEntry(*e));
        }
    QueryTemplate::EntryMapping const& em;
    QueryTemplate& qt;
};

////////////////////////////////////////////////////////////////////////
// QueryTemplate::Entry subclasses
////////////////////////////////////////////////////////////////////////
std::string QueryTemplate::TableEntry::getValue() const {
    std::stringstream ss;
    if(!db.empty()) { ss << db << "."; }
    ss << table;
    return ss.str();
}

class ColumnEntry : public QueryTemplate::Entry {
public:
    ColumnEntry(query::ColumnRef const& cr)
        : db(cr.db), table(cr.table), column(cr.column) {
    }
    virtual std::string getValue() const {
        std::stringstream ss;
        if(!db.empty()) { ss << db << "."; }
        if(!table.empty()) { ss << table << "."; }
        ss << column;
        return ss.str();
    }
    virtual bool isDynamic() const { return true; }

    std::string db;
    std::string table;
    std::string column;
};
struct EntryMerger {
    EntryMerger() {}

    void operator()(std::shared_ptr<QueryTemplate::Entry> e) {
        if(!_candidates.empty()) {
            if(!_checkMergeable(_candidates.back(), e)) {
                _mergeCurrent();
            }
        }
        _candidates.push_back(e);
    }
    void pack() { _mergeCurrent(); }
    bool _checkMergeable(std::shared_ptr<QueryTemplate::Entry> left,
                         std::shared_ptr<QueryTemplate::Entry> right) {
        return !((left->isDynamic() || right->isDynamic()));
    }
    void _mergeCurrent() {
        if(_candidates.size() > 1) {
            std::shared_ptr<QueryTemplate::Entry> e;
            e = std::make_shared<QueryTemplate::StringEntry>(
                                                   outputString(_candidates)
                                                             );
            _entries.push_back(e);
            _candidates.clear();
        } else if(!_candidates.empty()) {
            // Only one entry.
            _entries.push_back(_candidates.back());
            _candidates.pop_back();
        }
    }
    QueryTemplate::EntryPtrVector _candidates;
    QueryTemplate::EntryPtrVector _entries;
};

////////////////////////////////////////////////////////////////////////
// QueryTemplate
////////////////////////////////////////////////////////////////////////
std::string
QueryTemplate::toString() const {
    return outputString(_entries);
}

void
QueryTemplate::append(std::string const& s) {
    std::shared_ptr<Entry> e = std::make_shared<StringEntry>(s);
    _entries.push_back(e);
}

void
QueryTemplate::append(query::ColumnRef const& cr) {
    std::shared_ptr<Entry> e = std::make_shared<ColumnEntry>(cr);
    _entries.push_back(e);
}

void
QueryTemplate::append(TableEntry const& te) {
    std::shared_ptr<Entry> e = std::make_shared<TableEntry>(te);
    _entries.push_back(e);
}

void
QueryTemplate::append(std::shared_ptr<QueryTemplate::Entry> const& e) {
    _entries.push_back(e);
}

std::string
QueryTemplate::generate() const {
    return outputString(_entries);
}

std::string
QueryTemplate::generate(EntryMapping const& em) const {
    QueryTemplate newQt;
    std::for_each(_entries.begin(), _entries.end(), MappingWrapper(em, newQt));
    return outputString(newQt._entries);
}

void
QueryTemplate::clear() {
    _entries.clear();
}

////////////////////////////////////////////////////////////////////////
// QueryTemplate (private)
////////////////////////////////////////////////////////////////////////
void
QueryTemplate::optimize() {

    typedef EntryPtrVector::const_iterator Iter;

    EntryMerger em;
    for(Iter i=_entries.begin(); i != _entries.end(); ++i) {
        em(*i);
    }
    em.pack();
    _entries.swap(em._entries);
    //LOGF_DEBUG("merged %1% entries to %2%"
    //           % _entries.size() % em._entries.size());
    //LOGF_DEBUG("was: %1%" % outputString(_elements));
    //LOGF_DEBUG("now: %1%" % outputString(em._entries));
}

}}} // namespace lsst::qserv::query
