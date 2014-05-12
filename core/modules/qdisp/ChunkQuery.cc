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
// class ChunkQuery represents a query regarding a single
// chunk. Operates using a state-machine approach and transitions upon
// events/callbacks.  See ChunkQuery.h

// ReadCallable and WriteCallable are workqueue object callbacks that
// allow chunkQuery work to be done in a workqueue(thread pool).

// System headers
#include <fcntl.h> // for O_RDONLY, O_WRONLY, etc.
#include <iostream>

// Third-party headers
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include "XrdPosix/XrdPosixXrootd.hh"

// Local headers
#include "control/AsyncQueryManager.h"
#include "control/DynamicWorkQueue.h"
#include "log/msgCode.h"
#include "log/Logger.h"
#include "qdisp/ChunkQuery.h"
#include "qdisp/MessageStore.h"
#include "util/xrootd.h"
#include "util/StringHash.h"
#include "xrdc/PacketIter.h"

namespace lsst {
namespace qserv {
namespace qdisp {

#define DEBUG 2

namespace {

    void errnoComplain(char const* desc, int num, int err) {
        char buf[256];
        buf[0] = '\0';
        char * s = ::strerror_r(err, buf, sizeof(buf));
        buf[sizeof(buf) - 1] = '\0';
        LOGGER_INF << desc << ": " << num << " " << (s ? s : "") << std::endl;
    }

    int closeFd(int fd,
                std::string const& desc,
                std::string const& comment,
                std::string const& comment2) {
        LOGGER_INF << (std::string() + "Close (" + desc + ") of "
                      + boost::lexical_cast<std::string>(fd)  + " "
                      + comment) << std::endl;
        int res = xrdc::xrdClose(fd);
        if (res != 0) {
            errnoComplain(("Faulty close " + comment2).c_str(), fd, errno);
        }
        return res;
    }

}  // anonymous namespace

//////////////////////////////////////////////////////////////////////
// class ChunkQuery::WriteCallable
//////////////////////////////////////////////////////////////////////
class ChunkQuery::WriteCallable : public control::DynamicWorkQueue::Callable {
public:
    explicit WriteCallable(ChunkQuery& cq) :
        _cq(cq)
    {}
    virtual ~WriteCallable() {}
    virtual void operator()() {
        LOGGER_DBG << "EXECUTING ChunkQuery::WriteCallable::operator()()" << std::endl;
        try {
            // Use blocking calls to prevent implicit thread creation by
            // XrdClient
            _cq._state = ChunkQuery::WRITE_OPEN;
            int tries = 5; // Arbitrarily try 5 times.
            int result;
            while (tries > 0) {
                --tries;
                result = xrdc::xrdOpen(_cq._spec.path.c_str(), O_WRONLY);
                if (result == -1) {
                    if (errno == ENOENT) {
                        std::stringstream msgStrm;
                        msgStrm << std::string("Chunk not found for path:")
                                << _cq._spec.path << " , "
                                << tries << " tries left ";
                        _cq._manager->getMessageStore()->addMessage(_cq._id,
                                          log::MSG_XRD_OPEN_FAIL, msgStrm.str());
                        continue;
                    }
                    _cq._manager->getMessageStore()->addMessage(_cq._id,
                        errno != 0 ? -abs(errno) : -1,
                        "Remote I/O error during XRD open for write.");
                    result = -errno;
                }
                break;
            }
            _cq.Complete(result);
        } catch (const char *msg) {
            _cq._state = ChunkQuery::ABORTED;
            _cq._manager->getMessageStore()->addMessage(_cq._id,
                 errno != 0 ? -abs(errno) : -1, msg);
            _cq._notifyManager();
        }
    }
    virtual void abort() {
        // Can't really squash myself.
    }

private:
    ChunkQuery& _cq;
};

//////////////////////////////////////////////////////////////////////
// class ChunkQuery::ReadCallable
//////////////////////////////////////////////////////////////////////
class ChunkQuery::ReadCallable : public control::DynamicWorkQueue::Callable {
public:
    explicit ReadCallable(ChunkQuery& cq) :
        _cq(cq), _isRunning(false)
    {}
    virtual ~ReadCallable() {} // Must halt current operation.
    virtual void operator()() {
        LOGGER_DBG << "EXECUTING ChunkQuery::ReadCallable::operator()()" << std::endl;
        try {
            // Use blocking reads to prevent implicit thread creation by
            // XrdClient
            _cq._state = ChunkQuery::READ_OPEN;
            _cq._readOpenTimer.start();
            _isRunning = true;
            int result = xrdc::xrdOpen(_cq._resultUrl.c_str(), O_RDONLY);
            if(result == -1 ) {
                LOGGER_WRN << "XRD open returned error." << std::endl;
                if(errno == EINPROGRESS) {
                    LOGGER_ERR << "Synchronous open returned EINPROGRESS!!!! "
                               << _cq._spec.chunkId
                               << std::endl;
                }
                _cq._attempts += 1;
                LOGGER_WRN << "ChunkQuery attempts =" << _cq._attempts << std::endl;
                if (_cq._attempts < _cq.MAX_ATTEMPTS) {
                    _cq._state = WRITE_QUEUE;
                    _cq._manager->addToWriteQueue(new WriteCallable(_cq));
                } else {
                    _cq._result.read = -errno;
                    _cq._state = ChunkQuery::COMPLETE;
                    _cq._manager->getMessageStore()->addMessage(_cq._id,
                        errno != 0 ? -abs(errno) : -1,
                        "Remote I/O error during XRD open for read.");
                    _cq._notifyManager();
                }
                return;
            }
            _cq.Complete(result);
        } catch (const char *msg) {
            _cq._state = ChunkQuery::ABORTED;
            _cq._manager->getMessageStore()->addMessage(_cq._id,
                errno != 0 ? -abs(errno) : -1, msg);
            _cq._notifyManager();
        }
    }
    virtual void abort() {
        if(_isRunning) {
            // This is the best we can do for squashing.
            _cq._unlinkResult(_cq._resultUrl);
        }
    }

private:
    ChunkQuery& _cq;
    bool _isRunning;
};

//////////////////////////////////////////////////////////////////////
// class ChunkQuery
//////////////////////////////////////////////////////////////////////
char const* ChunkQuery::getWaitStateStr(WaitState s) {
    switch(s) {
    case WRITE_QUEUE: return "WRITE_QUEUE";
    case WRITE_OPEN: return "WRITE_OPEN";
    case WRITE_WRITE : return "WRITE_WRITE";
    case READ_QUEUE: return "READ_QUEUE";
    case READ_OPEN: return "READ_OPEN";
    case READ_READ: return "READ_READ";
    case COMPLETE : return "COMPLETE ";
    case CORRUPT: return "CORRUPT";
    case ABORTED: return "ABORTED";
    default:
        return "**UNKNOWN**";
    }
}

void ChunkQuery::Complete(int Result) {
    LOGGER_DBG << "EXECUTING ChunkQuery::Complete(" << Result << ")" << std::endl;
    // Prevent multiple Complete() callbacks from stacking.
    boost::shared_ptr<boost::mutex> m(_completeMutexP);
    boost::lock_guard<boost::mutex> lock(*m);

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
            LOGGER_WRN << "Problem reading result: open returned "
                       << _result.read << " for chunk=" << _spec.chunkId
                       << " with url=" << _resultUrl
                       << std::endl;
            isReallyComplete = true;
            _state = COMPLETE;
        } else {
            _state = READ_READ;
            //_manager->getReadPermission();
            //_readResults(Result);
            _readResultsDefer(Result);
        }
        break;
    default:
        isReallyComplete = true;
        ss << "Bad transition (likely bug): ChunkQuery @ " << _state
           << " Complete() -> CORRUPT " << CORRUPT << std::endl;
        _state = CORRUPT;
    }
    if(isReallyComplete) { _notifyManager(); }
    LOGGER_INF << ss.str();
}

ChunkQuery::ChunkQuery(control::TransactionSpec const& t, int id,
                       control::AsyncQueryManager* mgr)
    : XrdPosixCallBack(),
      _id(id), _spec(t),
      _manager(mgr),
      _shouldSquash(false) {
    if(!_manager) {
        throw std::invalid_argument("Null AsyncQueryMsnager");
    }
    _result.open = 0;
    _result.queryWrite = 0;
    _result.read = 0;
    _result.localWrite = 0;
    _attempts = 0;
    _hash = lsst::qserv::util::StringHash::getMd5Hex(_spec.query.c_str(),
                                                     _spec.query.size());
    // Patch the spec to include the magic query terminator.
    _spec.query.append(4,0); // four null bytes.
    _completeMutexP.reset(new boost::mutex);
}

ChunkQuery::~ChunkQuery() {
    LOGGER_DBG << "ChunkQuery (" << _id << ", " << _hash
               << "): Goodbye!" << std::endl;
}

void ChunkQuery::run() {
    LOGGER_DBG << "EXECUTING ChunkQuery::run()" << std::endl;
    // This lock ensures that the remaining ChunkQuery::Complete() calls
    // do not proceed until this initial step completes.
    boost::unique_lock<boost::mutex> lock(_mutex);
    LOGGER_INF << "Opening " << _spec.path << "\n";
    _writeOpenTimer.start();
#if 0
    int result = 0;
    while(true) {
        result = xrdc::xrdOpenAsync(_spec.path.c_str(), O_WRONLY, this);
        if(result == -EMFILE) {
            _manager->signalTooManyFiles();
            _manager->getWritePermission();
        } else {
            break;
        }
    }
    if(result != -EINPROGRESS) {
        // don't continue, set result with the error.
        LOGGER_ERR << "Not EINPROGRESS (" << result
                   << "), should not continue with "
                   << _spec.path << "\n";
        _result.open = result;
        _state = COMPLETE;
        _notifyManager(); // manager should delete me.
    } else {
        LOGGER_INF << "Waiting for " << _spec.path << "\n";
    }
#elif 1
    _state = WRITE_QUEUE;
    _manager->addToWriteQueue(new WriteCallable(*this));
#else     //synchronous open:
    result = xrdc::xrdOpen(_spec.path.c_str(), O_WRONLY);
    if (result == -1) {
        result = -errno;
    }
    lock.release()->unlock();
    Complete(result);
#endif
    // Callback(Complete) will handle the rest.
}

std::string ChunkQuery::getDesc() const {
    std::stringstream ss;
    ss << "Query " << _id << " (" << _hash << ") " << _resultUrl
       << " " << _queryHostPort << " state=";
    switch(_state) {
    case WRITE_QUEUE:
        ss << "queuedWrite";
        break;
    case WRITE_OPEN:
        ss << "openingWrite";
        break;
    case WRITE_WRITE:
        ss << "writing";
        break;
    case READ_QUEUE:
        ss << "queuedRead";
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

boost::shared_ptr<xrdc::PacketIter>
ChunkQuery::getResultIter() {
    return _packetIter;
}

void ChunkQuery::requestSquash() {
    LOGGER_DBG << "Squash requested for (" << _id << ", " << _hash << ")" << std::endl;
    _shouldSquash = true;
    switch(_state) {
    case WRITE_QUEUE: // Write is queued...
        //FIXME: Remove the job from the work queue.
        // Actually, should just assume that other code will be clearing the queue.
        break;
    case WRITE_OPEN:
        // Do nothing. Will get squashed at callback.
        break;
    case WRITE_WRITE:
        // Do nothing. After write completes, it will check the squash flag.
        break;
    case READ_QUEUE:
        // Assume job will be cleared from its queue.
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
    case ABORTED:
        break; // Already squashed?
    case CORRUPT:
    default:
        // Something's screwed up.
        LOGGER_ERR << "ChunkQuery squash failure. Bad state="
                   << getWaitStateStr(_state) << std::endl;
        // Not sure what we can do.
        break;
    }
}

void ChunkQuery::_squashAtCallback(int result) {
    LOGGER_DBG << "Squashing at callback (" << _id << ", " << _hash << ")" << std::endl;
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
        res = xrdc::xrdClose(result);
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
        res = xrdc::xrdClose(result);
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
        LOGGER_ERR << "Unexpected state at squashing. Expecting READ_OPEN "
                   << "or WRITE_OPEN, got:" << getDesc() << std::endl;
    }
}

bool ChunkQuery::_openForRead(std::string const& url) {
    _state = READ_OPEN;
    LOGGER_DBG << "opening async read to " << url << "\n";
    _readOpenTimer.start();
    _result.read = xrdc::xrdOpenAsync(url.c_str(), O_RDONLY, this);
    LOGGER_DBG << "Async read for " << _hash << " got " << _result.read
               << " --> "
               << ((_result.read == -EINPROGRESS) ? "ASYNC OK" : "fail?")
               << std::endl;
    return _result.read == -EINPROGRESS; // -EINPROGRESS is successful.
}

void ChunkQuery::_sendQuery(int fd) {
    std::stringstream ss;
    bool isReallyComplete = false;
    // Now write
    int len = _spec.query.length();
    _writeTimer.start();
    int writeCount = xrdc::xrdWrite(fd, _spec.query.c_str(), len);
    if (writeCount < 0) {
        throw "Remote I/O error during XRD write.";
    }
    _writeTimer.stop();
    ss << _hash << " WriteQuery " << _writeTimer << std::endl;
    _manager->getMessageStore()->addMessage(_id, log::MSG_XRD_WRITE,
                                            "Query Written.");

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
        _queryHostPort = xrdc::xrdGetEndpoint(fd);
        _resultUrl = util::makeUrl(_queryHostPort.c_str(), "result", _hash, 'r');
        _writeCloseTimer.start();
        closeFd(fd, "Normal", "dumpPath " + _spec.savePath,
                "post-dispatch");
        _writeCloseTimer.stop();
        ss << _hash << " QuerySize " << len << std::endl;
        ss << _hash << " WriteClose " << _writeTimer << std::endl;

        if(_shouldSquash) {
            _unlinkResult(_resultUrl);
            isReallyComplete = true;
        } else {
#if 1
            _state = READ_QUEUE;
            // Only attempt opening the read if not squashing.
            _manager->addToReadQueue(new ReadCallable(*this));
#else
            if(!_openForRead(_resultUrl)) {
                isReallyComplete = true;
            }
#endif
        }
    } // Write ok
    if(isReallyComplete) {
        _state=COMPLETE;
        _notifyManager();
    }
    LOGGER_INF << ss.str();
}

void ChunkQuery::_readResultsDefer(int fd) {
    LOGGER_DBG << "EXECUTING ChunkQuery::_readResultsDefer(" << fd << ")" << std::endl;
    int const fragmentSize = 4*1024*1024; // 4MB fragment size (param?)
    // Should limit cumulative result size for merging.  Now is a
    // good time. Configurable, with default=1G?

    // Now read.
    // packetIter will close fd
    _packetIter.reset(new xrdc::PacketIter(fd, fragmentSize));
    _result.localWrite = 1; // MAGIC: stuff the result so that it doesn't
    // look like an error to skip the local write.
    _state = COMPLETE;
    _manager->getMessageStore()->addMessage(_id, log::MSG_XRD_READ,
                                            "Results Read.");
    LOGGER_INF << _hash << " ReadResults defer " << std::endl;
    _notifyManager();
}

void ChunkQuery::_readResults(int fd) {
    int const fragmentSize = 4*1024*1024; // 4MB fragment size (param?)
    // Should limit cumulative result size for merging.  Now is a
    // good time. Configurable, with default=1G?

    // Now read.
    _readTimer.start();
    xrdc::xrdReadToLocalFile(fd, fragmentSize, _spec.savePath.c_str(),
                             &_shouldSquash, &(_result.localWrite),
                             &(_result.read));
    _readTimer.stop();
    LOGGER_INF << _hash << " ReadResults " << _readTimer << std::endl;
    _readCloseTimer.start();
    int res = xrdc::xrdClose(fd);
    _readCloseTimer.stop();
    LOGGER_INF << _hash << " ReadClose " << _readTimer << std::endl;
    if(res != 0) {
        errnoComplain("Error closing after result read", fd, errno);
    }
    LOGGER_INF << _spec.chunkId << " "
               << _hash << " -- wrote " << _result.localWrite
               << " read " << _result.read << std::endl;
    _state = COMPLETE;
    _notifyManager(); // This is a successful completion.
}

void ChunkQuery::_notifyManager() {
    bool aborted = (_state==ABORTED)
        || _shouldSquash
        || (_result.queryWrite < 0);
    LOGGER_DBG << "cqnotify " << _id  << " " << (void*) _manager << std::endl;
    _manager->finalizeQuery(_id, _result, aborted);
}

void ChunkQuery::_unlinkResult(std::string const& url) {
    int res = XrdPosixXrootd::Unlink(url.c_str());
    // FIXME: decide how to handle error here.
    if(res == -1) {
        res = errno;
        LOGGER_ERR << "ChunkQuery abort error: unlink gave errno = "
                   << res << std::endl;
    }
}

}}} // namespace lsst::qserv::qdisp

