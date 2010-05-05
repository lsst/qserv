// Standard
#include <iostream>
#include <sstream>
#include <list>
#include <deque>

// Boost
#include "boost/shared_ptr.hpp"

// ANTLR
#include "antlr/AST.hpp"
#include "antlr/CommonAST.hpp"

// Package
#include "lsst/qserv/master/parser.h"
#include "lsst/qserv/master/AggregateMgr.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/Templater.h"

// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2Lexer.hpp"

namespace qMaster = lsst::qserv::master;
using std::stringstream;
using qMaster::walkTreeString;


namespace { // Anonymous

class SqlParseRunner {
public:
    SqlParseRunner(std::string const& statement, std::string const& delimiter) :
	_statement(statement),
	_stream(statement, stringstream::in | stringstream::out),
	_lexer(_stream),
	_parser(_lexer),
	_delimiter(delimiter),
	_templater(_delimiter)
    { }

    void setup(std::list<std::string> const& names) {
	_templater.setKeynames(names.begin(), names.end());
	_parser._columnRefHandler = _templater.getColumnHandler();
	_parser._qualifiedNameHandler = _templater.getTableHandler();
	_tableListHandler = _templater.getTableListHandler();
	_parser._tableListHandler = _tableListHandler;
	_parser._setFctSpecHandler = _aggMgr.getSetFuncHandler();
	_parser._aliasHandler = _aggMgr.getAliasHandler();
	_parser._selectListHandler = _aggMgr.getSelectListHandler();
	_parser._selectStarHandler = _aggMgr.getSelectStarHandler();
    }

    std::string getParseResult() {
	if(_errorMsg.empty() && _parseResult.empty()) {
	    _computeParseResult();
	}
	return _parseResult;
    }
    std::string getAggParseResult() {
	if(_errorMsg.empty() && _aggParseResult.empty()) {
	    _computeParseResult();
	}
	return _aggParseResult;
    }
    void _computeParseResult() {
	try {
	    _parser.initializeASTFactory(_factory);
	    _parser.setASTFactory(&_factory);
	    _parser.sql_stmt();
	    _aggMgr.postprocess();
	    RefAST ast = _parser.getAST();
	    if (ast) {
		std::cout << "fixupSelect " << getFixupSelect();
		std::cout << "passSelect " << getPassSelect();
		// ";" is not in the AST, so add it back.
		_parseResult = walkTreeString(ast) + ";";
		_aggMgr.applyAggPass();
		_aggParseResult = walkTreeString(ast) + ";";
	    } else {
		_errorMsg = "Error: no AST from parse";
	    }
	} catch( antlr::ANTLRException& e ) {
	    _errorMsg =  "Parse exception: " + e.getMessage();
	} catch( std::exception& e ) {
	    _errorMsg = std::string("General exception: ") + e.what();
	}
	return; // Error.
    }
    bool getHasChunks() const { 
	return _tableListHandler->getHasChunks();
    }
    bool getHasSubChunks() const { 
	return _tableListHandler->getHasSubChunks();
    }
    bool getNeedsFixup() const {
	return false; // FIXME
    }
    std::string getFixupSelect() {
	return _aggMgr.getFixupSelect();
    }
    std::string getPassSelect() {
	return _aggMgr.getPassSelect();
    }
    std::string const& getError() const {
	return _errorMsg;
    }
private:
    std::string _statement;
    std::stringstream _stream;
    ASTFactory _factory;
    SqlSQL2Lexer _lexer;
    SqlSQL2Parser _parser;
    std::string _delimiter;
    qMaster::Templater _templater;
    qMaster::AggregateMgr _aggMgr;
    boost::shared_ptr<qMaster::Templater::TableListHandler>  _tableListHandler;

    std::string _parseResult;
    std::string _aggParseResult;
    std::string _errorMsg;

};
} // Anonymous namespace

///////////////////////////////////////////////////////////////////////////
// class Substitution
///////////////////////////////////////////////////////////////////////////
Substitution::Substitution(std::string template_, std::string const& delim) 
    : _template(template_) {
    _build(delim);
}
    
std::string Substitution::transform(Mapping const& m) {
    // This can be made more efficient by pre-sizing the result buffer
    // copying directly into it, rather than creating
    // intermediate string objects and appending.
    //
    unsigned pos = 0;
    std::string result;
    // No re-allocations if transformations are constant-size.
    result.reserve(_template.size()); 

    for(std::vector<Item>::const_iterator i = _index.begin();
	i != _index.end(); ++i) {
	// Copy bits since last match
	result += _template.substr(pos, i->position - pos);
	// Copy substitution
	Mapping::const_iterator s = m.find(i->name);
	if(s == m.end()) {
	    result += i->name; // passthrough.
	} else {
	    result += s->second; // perform substitution
	}
	// Update position
	pos = i->position + i->length;
    }
    // Copy remaining.
    if(pos < _template.length()) {
	result += _template.substr(pos);
    }
    return result;
}

// Let delim = ***
// blah blah ***Name*** blah blah
//           |         |
//         pos       endpos
//           |-length-|
//        name = Name
void Substitution::_build(std::string const& delim) {
    //int maxLength = _max(names.begin(), names.end());
    int delimLength = delim.length();
    for(unsigned pos=_template.find(delim); 
	pos < _template.length(); 
	pos = _template.find(delim, pos+1)) {
	unsigned endpos = _template.find(delim, pos + delimLength);
	Item newItem;
	newItem.position = pos;
	newItem.length = (endpos - pos) + delimLength;
	newItem.name.assign(_template, pos + delimLength,
			    newItem.length - delimLength - delimLength);
	// Note: length includes two delimiters.
	_index.push_back(newItem);
	pos = endpos;

	// Sanity check:
	// Check to see if the name is in names.
	    
    }

}

///////////////////////////////////////////////////////////////////////////
// class SqlSubstitution
///////////////////////////////////////////////////////////////////////////
SqlSubstitution::SqlSubstitution(std::string const& sqlStatement, 
				 Mapping const& mapping) 
    : _delimiter("*?*") {
    _build(sqlStatement, mapping);
    //
}
    
std::string SqlSubstitution::transform(Mapping const& m) {
    return _substitution->transform(m);
}

void SqlSubstitution::_build(std::string const& sqlStatement, 
			     Mapping const& mapping) {
    // 
    std::string template_;

    Mapping::const_iterator end = mapping.end();
    std::list<std::string> names;
    for(Mapping::const_iterator i=mapping.begin(); i != end; ++i) {
	names.push_back(i->first);
    }
    SqlParseRunner spr(sqlStatement, _delimiter);
    spr.setup(names);
    if(spr.getNeedsFixup()) {
	template_ = spr.getAggParseResult();
    } else {
	template_ = spr.getParseResult();
    } 
    _computeChunkLevel(spr.getHasChunks(), spr.getHasSubChunks());

    if(template_.empty()) {
	_errorMsg = spr.getError();
    }
    _substitution = SubstPtr(new Substitution(template_, _delimiter));
	
}

void SqlSubstitution::_computeChunkLevel(bool hasChunks, bool hasSubChunks) {
    // SqlParseRunner's TableList handler will know if it applied 
    // any subchunk rules, or if it detected any chunked tables.

    if(hasChunks) {
	if(hasSubChunks) {
	    _chunkLevel = 2;
	} else {
	    _chunkLevel = 1;
	}
    } else {
	_chunkLevel = 0;
    }
}

///////////////////////////////////////////////////////////////////////////
//class ChunkMapping
///////////////////////////////////////////////////////////////////////////
ChunkMapping::Map ChunkMapping::getMapping(int chunk, int subChunk) {
    Map m;
    ModeMap::const_iterator end = _map.end();
    std::string chunkStr = _toString(chunk);
    std::string subChunkStr = _toString(subChunk);
    static const std::string one("1");
    static const std::string two("2");
    // Insert mapping for: plainchunk, plainsubchunk1, plainsubchunk2
    for(ModeMap::const_iterator i = _map.begin(); i != end; ++i) {
	if(i->second == CHUNK) {
	    m.insert(MapValue(i->first, i->first + "_" + chunkStr));
	} else if (i->second == CHUNK_WITH_SUB) {
	    // Object --> Object_chunk
	    // Object_s1 --> Object_chunk_subchunk
	    // Object_s2 --> Object_chunk_subchunk (FIXME: add overlap)
	    m.insert(MapValue(i->first, i->first + "_" + chunkStr));
	    m.insert(MapValue(i->first + _subPrefix + one, 
			      i->first + "_" + chunkStr + "_" + subChunkStr));
	    m.insert(MapValue(i->first + _subPrefix + two, 
			      i->first + "_" + chunkStr + "_" + subChunkStr));
	}
    }
    return m;
}

ChunkMapping::Map const& ChunkMapping::getMapReference(int chunk, int subChunk) {
    _instanceMap = getMapping(chunk, subChunk);
    return _instanceMap;
}
