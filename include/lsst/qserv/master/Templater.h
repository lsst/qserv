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

class Templater {
public:
    typedef std::map<std::string, int> IntMap;

    class ColumnHandler : public VoidFourRefFunc {
    public:    
	ColumnHandler(Templater& t) : _templater(t) {}
	virtual ~ColumnHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b, 
				antlr::RefAST c, antlr::RefAST d) {
	    if(d.get()) {
		_templater._processName(b, c);
	    } else if(c.get()) {
		_templater._processName(a, b);
	    } else if(b.get()) {
		_templater._processName(antlr::RefAST(), a);
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
    
    class TableHandler : public VoidThreeRefFunc {
    public: 
	TableHandler(Templater& t) : _templater(t) {}
	virtual ~TableHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b, antlr::RefAST c)  {
	    // right-most is the table name.
	    if(c.get()) {
		_templater._processName(b, c);
	    } else if(b.get()) {
		_templater._processName(a, b);
	    } else if(a.get()) {
		_templater._processName(antlr::RefAST(), a);
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

    class TableListHandler : public VoidTwoRefFunc {
    public: 
	TableListHandler(Templater& t) : _templater(t) {}
	virtual ~TableListHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b);
	bool getHasChunks() const { return _hasChunks; }
	bool getHasSubChunks() const { return _hasSubChunks; }
	IntMap const& getUsageCount() const { return _usageCount; }
    private:
	Templater& _templater;
	bool _hasChunks;
	bool _hasSubChunks;
	IntMap _usageCount;
    };

    typedef std::map<std::string, char> ReMap;
    
    Templater(std::string const& delimiter="*?*", 
              antlr::ASTFactory* factory=0, 
              std::string const& defaultDb=std::string());
    ~Templater() { }

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
private:
    void _processName(antlr::RefAST db, antlr::RefAST n); 
    
    ReMap _map;
    std::string _delimiter;
    antlr::ASTFactory* _factory;
    std::string const _defaultDb;

    // static const
    static std::string const _nameSep;

    // Scope exceptions
    friend class Templater::TableHandler;
    friend class Templater::ColumnHandler;
};

}}} // lsst::qserv::master

#endif // LSST_QSERV_MASTER_TEMPLATER_H
