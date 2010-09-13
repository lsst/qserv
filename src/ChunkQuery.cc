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
 

// Standard
#include <iostream>
#include <fcntl.h> // for O_RDONLY, O_WRONLY, etc.

// Boost
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

// Xrootd
#include "XrdPosix/XrdPosixXrootd.hh"

// Package
#include "lsst/qserv/master/ChunkQuery.h"
#include "lsst/qserv/master/xrootd.h"
#include "lsst/qserv/master/AsyncQueryManager.h"

// Namespace modifiers
using boost::make_shared;
namespace qMaster = lsst::qserv::master;

namespace {
    void errnoComplain(char const* desc, int num, int errn) {
        char buf[256];
        ::strerror_r(errno, buf, 256);
        buf[256]='\0';
        std::cout << desc << ": " << num << " " << buf << std::endl; 
    }
    int closeFd(int fd, 
                std::string const& desc, 
                std::string const& comment,
                std::string const& comment2) {
        std::cout << (std::string() + "Close (" + desc + ") of "
                      + boost::lexical_cast<std::string>(fd)  + " " 
                      + comment) << std::endl;
	int res = qMaster::xrdClose(fd); 
        if(res != 0) {
            errnoComplain(("Faulty close " + comment2).c_str(), fd, errno);
        }
    }

}


//////////////////////////////////////////////////////////////////////
// class ChunkQuery 
//////////////////////////////////////////////////////////////////////
void qMaster::ChunkQuery::Complete(int Result) {
    std::stringstream ss;
    bool isReallyComplete = false;
    if(_shouldSquash) {        
        _squashAtCallback(Result);
        return; // Anything else to do?
    }
    switch(_state) {
    case WRITE_OPEN: // Opened, so we can send off the query
        _writeOpenTimer.stop();
        ss << _hash << " WriteOpen " << _writeOpenTimer << std::endl;
	{
	    boost::lock_guard<boost::mutex> lock(_mutex);
	    _result.open = Result;
	}
	if(Result < 0) { // error? 
	    _result.open = Result;
	    isReallyComplete = true;
	    _state = COMPLETE;
	} else {
	    _state = WRITE_WRITE;
	    _sendQuery(Result);	   
	}
	break;
    case READ_OPEN: // Opened, so we can read-back the results.
        _readOpenTimer.stop();
        ss << _hash << " ReadOpen " << _readOpenTimer << std::endl;
	if(Result < 0) { // error? 
	    _result.read = Result;
	    std::cout << "Problem reading result: open returned " 
		      << _result.read << std::endl;
	    isReallyComplete = true;
	    _state = COMPLETE;
	} else {
	    _state = READ_READ;
	    _readResults(Result);
	}
	break;
    default:
	isReallyComplete = true;
        ss << "FIXME: ChunkQuery @ " << _state 
           << " Complete() -> CORRUPT " << CORRUPT << std::endl;
	_state = CORRUPT;
    }
    if(isReallyComplete) { _notifyManager(); }
    std::cout << ss.str();
}

qMaster::ChunkQuery::ChunkQuery(qMaster::TransactionSpec const& t, int id, 
				qMaster::AsyncQueryManager* mgr) 
    : _spec(t), _manager(mgr), _id(id), _shouldSquash(false), 
      XrdPosixCallBack() {
    assert(_manager != NULL);
    _result.open = 0;
    _result.queryWrite = 0;
    _result.read = 0;
    _result.localWrite = 0;
    // Patch the spec to include the magic query terminator.
    _spec.query.append(4,0); // four null bytes.

}

qMaster::ChunkQuery::~ChunkQuery() {
    // std::cout << "ChunkQuery (" << _id << ", " << _hash 
    //           << "): Goodbye!" << std::endl;
}

void qMaster::ChunkQuery::run() {
    // This lock ensures that the remaining ChunkQuery::Complete() calls
    // do not proceed until this initial step completes.
    boost::lock_guard<boost::mutex> lock(_mutex);

    _state = WRITE_OPEN;
    std::cout << "Opening " << _spec.path << "\n";
    _writeOpenTimer.start();
    int result = qMaster::xrdOpenAsync(_spec.path.c_str(), O_WRONLY, this);
    if(result != -EINPROGRESS) {
	// don't continue, set result with the error.
	std::cout << "Not EINPROGRESS, should not continue with " 
		  << _spec.path << "\n";
	_result.open = result;
	_state = COMPLETE;
	_notifyManager(); // manager should delete me.
    } else {
	std::cout << "Waiting for " << _spec.path << "\n";
	_hash = qMaster::hashQuery(_spec.query.c_str(), 
				   _spec.query.size());
	
    }
    // Callback(Complete) will handle the rest.
}

std::string qMaster::ChunkQuery::getDesc() const {
    std::stringstream ss;
    ss << "Query " << _id << " (" << _hash << ") " << _resultUrl
       << " " << _queryHostPort << " state=";
    switch(_state) {
    case WRITE_OPEN:
	ss << "openingWrite";
	break;
    case WRITE_WRITE:
	ss << "writing";
	break;
    case READ_OPEN:
	ss << "openingRead";
	break;
    case READ_READ:
	ss << "reading";
	break;
    case COMPLETE:
	ss << "complete";
	break;
    case CORRUPT:
	ss << "corrupted";
	break;
    case ABORTED:
	ss << "aborted/squashed";
	break;

    default:
	break;
    }
    return ss.str();
}

void qMaster::ChunkQuery::requestSquash() { 
    //std::cout << "Squash requested for (" << _id << ", " << _hash << ")" << std::endl;
    _shouldSquash = true; 
    switch(_state) {
    case WRITE_OPEN:
        // Do nothing. Will get squashed at callback.
	break;
    case WRITE_WRITE:
        // Do nothing. After write completes, it will check the squash flag.
        break;
    case READ_OPEN:
        // Squash with an unlink() call to the result file.
        _unlinkResult(_resultUrl); 
	break;
    case READ_READ:
	// Do nothing. Result is being read.  Reader will check squash flag.
	break;
    case COMPLETE:
        // Do nothing.  It's too late to squash
	break;
    case CORRUPT:
    default:
        // Something's screwed up.
        std::cout << "ChunkQuery squash failure. Bad state=" 
                  << _state << std::endl;
        // Not sure what we can do.
	break;
    }    
}


void qMaster::ChunkQuery::_squashAtCallback(int result) {
    //std::cout << "Squashing at callback (" << _id << ", " << _hash << ")" << std::endl;
    // squash this query so that it stops running.
    std::stringstream ss;
    bool badState = false;
    int res;
    if(result < 0) { // Fail, don't have to squash.
        _state = ABORTED;
        _notifyManager();
        return;
    }
    switch(_state) {
    case WRITE_OPEN:
        _writeOpenTimer.stop();
        ss << _hash << " WriteOpen* " << _writeOpenTimer << std::endl;
        // Just close the channel w/o sending a query.
        _writeCloseTimer.start();
	res = qMaster::xrdClose(result);
        _writeCloseTimer.stop();
        ss << _hash << " WriteClose* " << _writeCloseTimer << std::endl;
        if(res != 0) {
            errnoComplain("Bad close while squashing write open",result, errno);
        }
	break;
    case WRITE_WRITE:
	// Shouldn't get called in this state.
        badState = true;
	break;
    case READ_OPEN:
        // Close the channel w/o reading the result (which might be faulty)
        _readCloseTimer.start();
	res = qMaster::xrdClose(result);
        _readCloseTimer.stop();
        ss << _hash << " ReadClose* " << _readCloseTimer << std::endl;
        if(res != 0) {
            errnoComplain("Bad close while squashing read open",result, errno);
        }        
	break;
    case READ_READ:
	// Shouldn't get called in this state.
        badState = true;
	break;
    case COMPLETE:
        // Shouldn't get called here, but doesn't matter.
        badState = true;
	break;
    case CORRUPT:
        // Shouldn't get called here either.
        badState = true;
	break;
    default:
        // Unknown state.
        badState = true;
	break;
    }
    _state = ABORTED;
    _notifyManager();
    if(badState) {
        std::cout << "Unexpected state at squashing. Expecting READ_OPEN "
                  << "or WRITE_OPEN, got:" << getDesc() << std::endl;
    }
}
    
bool qMaster::ChunkQuery::_openForRead(std::string const& url) {
    _state = READ_OPEN;
    //std::cout  << "opening async read to " << url << "\n";
    _readOpenTimer.start();
    _result.read = qMaster::xrdOpenAsync(url.c_str(), 
                                       O_RDONLY, this);
    // std::cout << "Async read for " << _hash << " got " << _result.read
    //           << " --> " 
    //           << ((_result.read == -EINPROGRESS) ? "ASYNC OK" : "fail?")
    //           << std::endl;
    return _result.read == -EINPROGRESS; // -EINPROGRESS is successful.
}

void qMaster::ChunkQuery::_sendQuery(int fd) {
    std::stringstream ss;
    bool isReallyComplete = false;
    // Now write
    int len = _spec.query.length();
    _writeTimer.start();
    int writeCount = qMaster::xrdWrite(fd, _spec.query.c_str(), len);
    _writeTimer.stop();
    ss << _hash << " WriteQuery " << _writeTimer << std::endl;
    
    // Get rid of the query string to save space
    _spec.query.clear();
    int res;
    if(writeCount != len) {
	_result.queryWrite = -errno;
	isReallyComplete = true;
	// To be safe, close file anyway.
        _writeCloseTimer.start();
        ss << _hash << " WriteQuery " << _writeTimer << std::endl;
        closeFd(fd, "Error-caused", "dumpPath " + _spec.savePath, 
                "post-dispatch");
        _writeCloseTimer.stop();
        ss << _hash << " WriteClose " << _writeTimer << std::endl;
    } else {
	_result.queryWrite = writeCount;
	_queryHostPort = qMaster::xrdGetEndpoint(fd);
	_resultUrl = qMaster::makeUrl(_queryHostPort.c_str(), "result", 
				      _hash);
        _writeCloseTimer.start();
        closeFd(fd, "Normal", "dumpPath " + _spec.savePath, 
                "post-dispatch");
        _writeCloseTimer.stop();
        ss << _hash << " WriteClose " << _writeTimer << std::endl;

        if(_shouldSquash) {
            _unlinkResult(_resultUrl);
            isReallyComplete = true;
        } else {
            // Only attempt opening the read if not squashing.
            if(!_openForRead(_resultUrl)) {
                isReallyComplete = true;
            }  
        }
    } // Write ok
    if(isReallyComplete) { 
	_state=COMPLETE;
	_notifyManager(); 
    }
    std::cout << ss.str();
}

void qMaster::ChunkQuery::_readResults(int fd) {
	int const fragmentSize = 4*1024*1024; // 4MB fragment size (param?)
        // Should limit cumulative result size for merging.  Now is a
        // good time. Configurable, with default=1G?

	// Now read.
        _readTimer.start();
	qMaster::xrdReadToLocalFile(fd, fragmentSize, _spec.savePath.c_str(), 
                                    &_shouldSquash,
                                    &(_result.localWrite), &(_result.read));
        _readTimer.stop();
        std::cout << _hash << " ReadResults " << _readTimer << std::endl;
        _readCloseTimer.start();
	int res = qMaster::xrdClose(fd);
        _readCloseTimer.stop();
        std::cout << _hash << " ReadClose " << _readTimer << std::endl;
        if(res != 0) {
            errnoComplain("Error closing after result read", fd, errno);
        }
	_state = COMPLETE;
	_notifyManager(); // This is a successful completion.
}
    
void qMaster::ChunkQuery::_notifyManager() {
    bool aborted = (_state==ABORTED) 
        || _shouldSquash 
        || (_result.queryWrite < 0);
    //std::cout << "cqnotify " << _id  << " " << (void*) _manager 
    //<< std::endl;
    _manager->finalizeQuery(_id, _result, aborted);
}

void qMaster::ChunkQuery::_unlinkResult(std::string const& url) {
    int res = XrdPosixXrootd::Unlink(url.c_str());
}
