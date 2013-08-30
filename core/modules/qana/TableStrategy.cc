/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "qana/TableStrategy.h"
#include <deque>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/pointer_cast.hpp>
#include "log/Logger.h"
#include "meta/MetadataCache.h"
#include "query/FromList.h"
#include "query/TableRef.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/QueryContext.h"

#define CHUNKTAG "%CC%"
#define SUBCHUNKTAG "%SS%"
#define FULLOVERLAPSUFFIX "FullOverlap"

#define DEBUG 1

namespace { // File-scope helpers
}

namespace lsst {
namespace qserv {
namespace master {
class InvalidTableException : public std::logic_error {
public:
    InvalidTableException(char const* db, char const* table)
        : std::logic_error(std::string("Invalid table: ") + db + "." + table)
        {}
    InvalidTableException(std::string const& db, std::string const& table)
        : std::logic_error(std::string("Invalid table: ") + db + "." + table)
        {}
};

struct Tuple {
    Tuple(std::string const& db_,
          std::string const& prePatchTable_,
          std::string const& alias_,
          TableRef const* node_)
        : db(db_),
          prePatchTable(prePatchTable_),
          alias(alias_),
          chunkLevel(-1),
          node(node_) {}
    std::string db;
    std::list<std::string> tables; // permutation relies on len(tables)=1 or 2
    std::string prePatchTable;
    std::string alias;
    int allowed;
    int chunkLevel;
    TableRef const* node;
};
std::ostream& operator<<(std::ostream& s, Tuple const& t) {
    s << "Tuple("
      << "db=" + t.db + ","
      << "tables=[";
    std::copy(t.tables.begin(), t.tables.end(),
              std::ostream_iterator<std::string>(s, ","));
    s << "],"
      << "prePatchTable=" + t.prePatchTable + ","
      << "alias=" + t.alias + ","
      << "allowed=" << t.allowed << ","
      << "chunkLevel=" << t.chunkLevel << ","
      << "node=" << (void*)t.node
      << ")";
    return s;
}
typedef std::deque<Tuple> Tuples;
Tuple const&  tuplesFindByRefRO(Tuples const& tuples, TableRef const& t) {
    // FIXME: Switch to map and rethink the system
    typedef Tuples::const_iterator Iter;
    for(Iter i=tuples.begin(),e=tuples.end(); i != e; ++i) {
        if(i->node == &t) { return *i; }
    }
    throw std::logic_error("Not found in tuples (inplace)");
}
Tuple&  tuplesFindByRef(Tuples& tuples, TableRef const& t) {
    // FIXME: Switch to map and rethink the system
    typedef Tuples::iterator Iter;
    for(Iter i=tuples.begin(),e=tuples.end(); i != e; ++i) {
        if(i->node == &t) { return *i; }
    }
    throw std::logic_error("Not found in tuples (inplace)");
}
void printTuples(Tuples const& tuples, std::ostream& os) {
    typedef Tuples::const_iterator Iter;
    int n=0;
    for(Iter i=tuples.begin(), e=tuples.end(); i!=e; ++i) {
        if(n) { os << ","; }
        os << "[" << n << "]" << *i;
        ++n;
    }
}
////////////////////////////////////////////////////////////////////////
// Helper classes
////////////////////////////////////////////////////////////////////////
class TableNamer {
public:
    static std::string makeSubChunkDbTemplate(std::string const& db) {
        return "Subchunks_" + db + "_" CHUNKTAG;
    }

    static std::string makeOverlapTableTemplate(std::string const& table) {
        return table + FULLOVERLAPSUFFIX "_" CHUNKTAG "_" SUBCHUNKTAG;
    }

    static std::string makeChunkTableTemplate(std::string const& table) {
        return table + "_" CHUNKTAG;
    }

    static std::string makeSubChunkTableTemplate(std::string const& table) {
        return table + "_" CHUNKTAG "_" SUBCHUNKTAG;
    }
    /// @return overall chunk level.
    static int patchTuples(Tuples& tuples) {
        // Are multiple subchunked tables involved? Then do
        // overlap... which requires creating a query sequence.
        // For now, skip the sequence part.
        // TODO: need to refactor a bit to allow creating a sequence.

        // If chunked table count > 1, use highest chunkLevel and turn on
        // subchunking.
        Tuples::iterator i = tuples.begin();
        Tuples::iterator e = tuples.end();
        int chunkedCount = 0;
        for(; i != e; ++i) {
            if(i->chunkLevel > 0) {
                ++chunkedCount;
            }
        }
        // Turn on chunking with any chunk table
        int finalChunkLevel = chunkedCount ? 1 : 0;
        bool firstSubChunk = true;
        for(i = tuples.begin(); i != e; ++i) {
            std::string const& prePatch = i->prePatchTable;
            switch(i->chunkLevel) {
            case 0:
                i->tables.push_back(prePatch);
                break;
            case 1:
                i->tables.push_back(makeChunkTableTemplate(prePatch));
                break;
            case 2:
                if(chunkedCount > 1) {
                    i->db = makeSubChunkDbTemplate(i->db);
                    i->tables.push_back(makeSubChunkTableTemplate(prePatch));
                    if(firstSubChunk) {
                        firstSubChunk = false;
                        // Turn on subchunking
                        finalChunkLevel = 2;
                    } else {
                        i->tables.push_back(makeOverlapTableTemplate(prePatch));
                    }
                } else {
                    i->tables.push_back(makeChunkTableTemplate(prePatch));
                }
                break;
            default:
                if(i->allowed) {
                    throw std::logic_error("Unexpected chunkLevel=" +
                                           boost::lexical_cast<std::string>(i->chunkLevel));
                }
                break;
            }
        }
        return finalChunkLevel;
    }
};

inline void updateMappingFromTuples(QueryMapping& m, Tuples const& tuples) {
    // Look for subChunked tables
    for(Tuples::const_iterator i=tuples.begin();
        i != tuples.end(); ++i) {
        if(i->chunkLevel == 2) {
            std::string const& table = i->prePatchTable;
            if(table.empty()) {
                throw std::logic_error("Unknown prePatchTable in QueryMapping");
            }
            // Add them to the list of subchunk table dependencies
            m.insertSubChunkTable(table);
        }
    }
}

class TableStrategy::Impl {
public:
    Impl(QueryContext& context_) : context(context_) {}
    ~Impl() {}

    QueryContext& context;
    Tuples tuples;
    int chunkLevel;
};

class addTable : public TableRef::Func {
public:
    addTable(Tuples& tuples) : _tuples(tuples) { }
    void operator()(TableRef::Ptr t) {
        if(t.get()) { t->apply(*this); }
    }
    virtual void operator()(TableRef& t) {
        std::string table = t.getTable();
        if(table.empty()) {
            throw std::logic_error("Missing table in TableRef");
        }
        _tuples.push_back(Tuple(t.getDb(), table,
                                t.getAlias(), &t));
    }

private:
    Tuples& _tuples;
};
class updateChunkLevel {
public:
    updateChunkLevel(MetadataCache& metadata_)
        : metadata(metadata_)
        {}

    void operator()(Tuple& t) {
        t.allowed = metadata.checkIfContainsDb(t.db); // Db exists?
        if(t.allowed) { // Check table as well.
            t.allowed = metadata.checkIfContainsTable(t.db, t.prePatchTable);
        }
        if(t.allowed) {
            t.chunkLevel = metadata.getChunkLevel(t.db, t.prePatchTable);
            if(t.chunkLevel == -1) {
                t.allowed = false; // No chunk level found: missing/illegal.
                throw InvalidTableException(t.db, t.prePatchTable);
            }
        } else {
            throw InvalidTableException(t.db, t.prePatchTable);
        }
    }
    MetadataCache& metadata;
};
class inplaceComputeTable : public TableRef::Func {
public:
    // FIXME: How can we consolidate with computeTable?
    inplaceComputeTable(Tuples& tuples) :_tuples(tuples) {
    }
    virtual void operator()(TableRef::Ptr t) {
        t->apply(*this);
    }
    virtual void operator()(TableRef& t) {
        Tuple const& tuple = tuplesFindByRefRO(_tuples, t);
        t.setDb(tuple.db);
        t.setTable(tuple.tables.front());
    }
    Tuples& _tuples;
};
class computeTable {
public:
    computeTable(Tuples& tuples, int permutation)
        : _tuples(tuples), _permutation(permutation) {
        // Should already know how many permutations. 0 - (n-1)
    }
    TableRef::Ptr operator()(TableRef::Ptr t) {
        return visit(*t);
        // See if tuple matches table.
        // if t in tuples,
        // else, if simple, return copy
        // if not simple, visit both sides.

        // if match, replace. otherwise, it's compound. For now, just
        // visit both sides of the join.
    }

    inline TableRef::Ptr visit(TableRef const& t) {
        TableRef::Ptr newT = lookup(t, _permutation);
        if(!newT) {
            newT.reset(new TableRef(t.getDb(), t.getTable(), t.getAlias()));
            std::cout << "passthrough table: " << t.getTable() << std::endl;
        }
        JoinRefList const& jList = t.getJoins();
        typedef JoinRefList::const_iterator Iter;
        for(Iter i=jList.begin(), e=jList.end(); i != e; ++i) {
            JoinRef const& j = **i;
            TableRef::Ptr right = visit(*j.getRight());
            JoinRef::Ptr r(new JoinRef(right, j.getJoinType(),
                                       j.isNatural(),
                                       j.getSpec()->clone()));
            newT->addJoin(r);
        }
        return newT;
    }
    TableRef::Ptr lookup(TableRef const& t, int permutation) {
        Tuple const& tuple = tuplesFindByRefRO(_tuples, t);
        // Probably select one bit out of permutation, based on which
        // which subchunked table this is in the query.
        int i = _permutation & 1; // adjust bitshift depending on num
                                  // subchunked tables.
        std::string table;
        if(i == 0) {
            table = tuple.tables.front();
        } else {
            table = tuple.tables.back();
        }
        TableRef::Ptr newT(new TableRef(tuple.db,
                                        table,
                                        t.getAlias()));

        return newT;
    }

    TableRefListPtr _tableRefnList;
    Tuples& _tuples;
    int _permutation;
};

////////////////////////////////////////////////////////////////////////
// TableStrategy public
////////////////////////////////////////////////////////////////////////
TableStrategy::TableStrategy(FromList const& f,
                             QueryContext& context)
    : _impl(new Impl(context)) {
    _import(f);
}

boost::shared_ptr<QueryMapping> TableStrategy::exportMapping() {
    boost::shared_ptr<QueryMapping> qm(new QueryMapping());

    LOGGER_DBG << __FILE__ ": _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;
    switch(_impl->chunkLevel) {
    case 0:
        break;
    case 1:
        LOGGER_DBG << __FILE__ ": calling  addChunkMap()"
                   << std::endl;
        qm->insertChunkEntry(CHUNKTAG);
        break;
    case 2:
        LOGGER_DBG << __FILE__": calling  addSubChunkMap()"
                   << std::endl;
        qm->insertChunkEntry(CHUNKTAG);
        qm->insertSubChunkEntry(SUBCHUNKTAG);
        updateMappingFromTuples(*qm, _impl->tuples);
        break;
    default:
        break;
    }
    return qm;

}
/// @return permuation count: 1 :singleton count (no subchunking)
///
int TableStrategy::getPermutationCount() const {
    // Count permutations by iterating over all tuples and counting the
    // combinations.
    int permutations = 1;
    typedef Tuples::const_iterator Iter;
    // Easier to count via tuples (flat) rather than table ref list (non-flat)
    for(Iter i=_impl->tuples.begin(), e=_impl->tuples.end(); i != e; ++i) {
        permutations *= i->tables.size();
    }
    if(permutations > 2) {
        LOGGER_ERR << "ERROR! permutations > 2 (=" << permutations
<< ")" << std::endl;
        throw std::logic_error("Support for permuations > 2 is unimplemented");
    }
    return permutations;

}

TableRefListPtr TableStrategy::getPermutation(int permutation, TableRefList const& tList) {
    TableRefListPtr oList(new TableRefList()); //tList.size()));
    std::transform(tList.begin(), tList.end(),
                   std::back_inserter(*oList), computeTable(_impl->tuples, permutation));
    return oList;
}

void TableStrategy::setToPermutation(int permutation, TableRefList& p) {
    // ignore permutation for now.
    inplaceComputeTable ict(_impl->tuples);
    std::for_each(p.begin(), p.end(), ict);
}

////////////////////////////////////////////////////////////////////////
// TableStrategy private
////////////////////////////////////////////////////////////////////////
void TableStrategy::_import(FromList const& f) {
    // Read into tuples. Why is the original structure insufficient?
    // Because I want to annotate! The annotations make subsequent
    // reasonaing and analysis possible. So the point will be to
    // populate a data structure of annotations.

    // Iterate over FromList elements
    TableRefList const& tList = f.getTableRefList();
    addTable a(_impl->tuples);
    std::for_each(tList.begin(), tList.end(), a);
    updateChunkLevel ucl(*_impl->context.metadata);
    std::for_each(_impl->tuples.begin(), _impl->tuples.end(), ucl);

    _impl->chunkLevel = TableNamer::patchTuples(_impl->tuples);
    LOGGER_DBG << "TableStrategy::_import() : _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;
    if(!_impl->context.metadata) {
        throw std::logic_error("Missing context.metadata");
    }
    _updateContext();
}

void TableStrategy::_updateContext() {
    // Patch context with mapping.
    if(_impl->context.queryMapping.get()) {
        _impl->context.queryMapping->update(*exportMapping());
    } else {
        _impl->context.queryMapping = exportMapping();
    }
}
}}} // lsst::qserv::master
