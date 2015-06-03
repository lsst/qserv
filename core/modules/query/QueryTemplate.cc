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

namespace {

LOG_LOGGER getLogger() {
    static LOG_LOGGER logger = LOG_GET("lsst.qserv.query.QueryTemplate");
    return logger;
}


struct SpacedOutput {

    SpacedOutput(std::ostream& os_, std::string sep_=" ")
        : os(os_), sep(sep_) {}

    void operator()(std::shared_ptr<QueryTemplate::Entry> entry) {
        if(!entry) { throw std::invalid_argument("NULL QueryTemplate::Entry"); }
        //if(e->isDynamic()) { os << "(" << count << ")"; }

        std::string const& entry_str = entry->getValue();
        LOGF(getLogger(), LOG_LVL_TRACE, "entry: %1%" % entry_str);
        if(entry_str.empty()) return;

        if(!last_entry.empty() && sql::sqlShouldSeparate(last_entry, last_entry.back(), entry_str.at(0)))  {
            os << sep;
        }
        os << entry_str;
        last_entry = entry_str;
    }

    std::ostream& os;
    std::string last_entry;
    std::string sep;
};


std::string stringify(QueryTemplate::EntryPtrVector const& v) {
    std::stringstream ss;
    std::string str;
    SpacedOutput so(ss, " ");
    std::for_each(v.begin(), v.end(), so);
    str = ss.str();
    LOGF(getLogger(), LOG_LVL_TRACE, "EntryPtrVector: %1%" % str);
    return str;
}

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


////////////////////////////////////////////////////////////////////////
// QueryTemplate
////////////////////////////////////////////////////////////////////////


std::string QueryTemplate::toString() const {
	return stringify(_entries);
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
QueryTemplate::generate(EntryMapping const& em) const {
    QueryTemplate newQt;
    std::for_each(_entries.begin(), _entries.end(), MappingWrapper(em, newQt));
    return newQt.toString();
}

void
QueryTemplate::clear() {
    _entries.clear();
}


}}} // namespace lsst::qserv::query
