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
 
#ifndef LSST_QSERV_MASTER_TEMPLATER_H
#define LSST_QSERV_MASTER_TEMPLATER_H

#include <deque>
#include <iostream>
#include <map>


#include <boost/shared_ptr.hpp>

#include "lsst/qserv/master/parserBase.h"

// Forward
namespace antlr {
    class ASTFactory; 
}

namespace lsst {
namespace qserv {
namespace master {
class TableRefChecker; // Forward
typedef boost::shared_ptr<TableRefChecker const> RefCheckerConstPtr;

/// class Templater : A templating module that helps produce string
/// templates for substitution for making SQL subqueries.  Manages db
/// whitelist for access control.
class Templater {
public:
    typedef std::map<std::string, int> IntMap;
    //typedef std::set<std::string> StringSet;
    //typedef std::map<StringSet> StrStrSet;

    // ColumnHandler: hooks into parser's production for column name references.
    class ColumnHandler : public VoidFourRefFunc {
    public:    
	ColumnHandler(Templater& t) : _templater(t) {}
	virtual ~ColumnHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b, 
				antlr::RefAST c, antlr::RefAST d) {
	    if(d.get()) {
		_templater._processLater(b, c);
	    } else if(c.get()) {
		_templater._processLater(a, b);
	    } else if(b.get()) {
		_templater._processLater(antlr::RefAST(), a);
	    }
	    // std::cout << "col _" << tokenText(a) 
	    // 	      << "_ _" << tokenText(b) 
	    // 	      << "_ _" << tokenText(c) 
	    // 	      << "_ _" << tokenText(d) 
	    // 	      << "_ "; 
	    // a->setText("AWESOMECOLUMN");
	}
    private:
	Templater& _templater;
	
    };
    // TableHandler: hooks into parser's production for table name references.
    class TableHandler : public VoidThreeRefFunc {
    public: 
	TableHandler(Templater& t) : _templater(t) {}
	virtual ~TableHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b, antlr::RefAST c)  {
	    // right-most is the table name.
	    if(c.get()) {
		_templater._processLater(b, c);
	    } else if(b.get()) {
		_templater._processLater(a, b);
	    } else if(a.get()) {
		_templater._processLater(antlr::RefAST(), a);
	    }
	    // std::cout << "qualname " << tokenText(a) 
	    // 	      << " " << tokenText(b) << " " 
	    // 	      << tokenText(c) << " "; 
	}
    private:
	Templater& _templater;
    };
    struct TypeVisitor {
    public:
	void operator()(antlr::RefAST& a) {
	    std::cout << "(" << a->getText() << " " 
		      << a->getType() << " " 
		      << a->typeName()
		      << ") " << std::endl;
	}
    };
    /// Defers a visit to nodes so it can be done later.
    /// This is useful for delaying processing for a set of nodes until 
    /// other conditions are satisfied.
    class DeferredVisitor {
    public:
	DeferredVisitor() {}
	void operator()(antlr::RefAST& a) { _q.push_back(a); }
        template <class Action>
        void visit(Action& a) {
            std::for_each(_q.begin(), _q.end(), a);
            _q.clear();
        }
    private:
        std::deque<antlr::RefAST> _q;
    };
    class JoinVisitor {
    public:
	JoinVisitor(std::string delim, std::string subPrefix) 
	    : _delim(delim), _subPrefix(subPrefix),
	      _hasChunks(false), _hasSubChunks(false) 
	{}
	void operator()(antlr::RefAST& a);
	void applySubChunkRule();
	bool getHasChunks() const { return _hasChunks; }
	bool getHasSubChunks() const { return _hasSubChunks; }
	IntMap getUsageCount() const;
    private:
	typedef std::deque<antlr::RefAST> RefList;
	typedef RefList::iterator RefListIter;
	typedef std::map<std::string, RefList> RefMap;
	typedef RefMap::value_type RefMapValue;
	typedef RefMap::iterator RefMapIter;
	typedef RefMap::const_iterator RefMapConstIter;

	void _addRef(antlr::RefAST& a);
	bool _isDelimited(std::string const& s);
	void _reassignRefs(RefList& l);
	RefMap _map;
	std::string _delim;
	std::string _subPrefix;
	// May want to store chunk table names and subchunk table names.
	bool _hasChunks;
	bool _hasSubChunks;
    };
    // TableListHandler - hooks into parser's production of table
    // lists (in FROM clauses)
    class TableListHandler : public VoidTwoRefFunc {
    public: 
	TableListHandler(Templater& t) : _templater(t) {}
	virtual ~TableListHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b);
        void processJoin();
	bool getHasChunks() const { return _hasChunks; }
	bool getHasSubChunks() const { return _hasSubChunks; }
	IntMap const& getUsageCount() const { return _usageCount; }
    private:
	Templater& _templater;
	bool _hasChunks;
	bool _hasSubChunks;
	IntMap _usageCount;
        DeferredVisitor _deferred;
    };
    // SpatialTableNameNotifier
    class Notifier {
    public:
        virtual void operator()(std::string const& refName, 
                                std::string const& name) {};
        virtual ~Notifier() {}
        static Notifier nullInstance;
    };
    // Templater 
    typedef std::map<std::string, char> ReMap;
    typedef std::deque<std::string> StringList;
    
    Templater(std::string const& delimiter="*?*", 
              antlr::ASTFactory* factory=0);
    ~Templater() { }
    void setup(IntMap const& dbWhiteList=IntMap(),
               RefCheckerConstPtr refChecker=RefCheckerConstPtr(),
               std::string const& defaultDb=std::string());


    template <typename Iter>
    void setKeynames(Iter begin, Iter end) {
	// Clear the map, then fill it.
	_map.clear();
	for(Iter i = begin; i != end; ++i) {
	    _map[*i] = true;
	}
    }

    std::string mungeName(std::string const& name) {
	return _delimiter + name + _delimiter;
    }
    void processNames();
    void signalFromStmtBegin();
    void signalFromStmtEnd();

    bool isSpecial(std::string const& s) {
	return _map.find(s) != _map.end();
    }
    boost::shared_ptr<TableHandler> newTableHandler() {
	return boost::shared_ptr<TableHandler>(new TableHandler(*this));
    }
    boost::shared_ptr<ColumnHandler> newColumnHandler() {
	return boost::shared_ptr<ColumnHandler>(new ColumnHandler(*this));
    }
    boost::shared_ptr<TableListHandler> newTableListHandler() {
	return boost::shared_ptr<TableListHandler>(new TableListHandler(*this));
    }

    std::string const& getDelimiter() const { return _delimiter; 
    }

//    TableRefChecker const& getTableRefChecker() const;

    StringList const& getBadDbs() const { return _badDbs; }
    void addGoodDb(std::string const& db) { _dbWhiteList[db] = 1; }
    class addAliasFunc {
    public:
        addAliasFunc(Templater& t) : _t(t) {}
        void operator()(std::string const& aName);
        Templater& _t;
    };
    friend class addAliasFunc;
private:
    typedef std::pair<antlr::RefAST, antlr::RefAST> RefAstPair ; 
    typedef std::deque<RefAstPair> RefPairQueue;

    bool _isAlias(std::string const& alias);
    bool _isDbOk(std::string const& db);
    void _markBadDb(std::string const& db);
    void _processLater(antlr::RefAST db, antlr::RefAST n);
    void _processName(RefAstPair& n);
    //void _markSpecial(std::string const& db, std::string const& table);

    RefPairQueue _columnProcessQueue;
    RefPairQueue _tableProcessQueue;
    ReMap _map;
    IntMap _dbWhiteList;
    IntMap _tableAliases;
    std::string _delimiter;
    antlr::ASTFactory* _factory;
    std::string _defaultDb;
    StringList _badDbs;
    boost::shared_ptr<TableRefChecker const> _refChecker;
    bool _fromStmtActive;
    bool _shouldDefer;
    
    // static const
    static std::string const _nameSep;

    // Scope exceptions
    friend class Templater::TableHandler;
    friend class Templater::ColumnHandler;
};

}}} // lsst::qserv::master

#endif // LSST_QSERV_MASTER_TEMPLATER_H
