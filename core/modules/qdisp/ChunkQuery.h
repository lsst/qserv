// -*- LSST-C++ -*-
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
// class ChunkQuery represents a query regarding a single chunk.
#ifndef LSST_QSERV_MASTER_CHUNKQUERY_H
#define LSST_QSERV_MASTER_CHUNKQUERY_H

// Scalla/xrootd
#include "XrdPosix/XrdPosixCallBack.hh"

// Package
#include "control/transaction.h"
#include "xrdc/xrdfile.h"
#include "util/Timer.h"

namespace lsst {
namespace qserv {
namespace master {

// forward
class AsyncQueryManager;
class PacketIter;

//////////////////////////////////////////////////////////////////////
// class ChunkQuery
// Handles chunk query execution, like openwritereadsaveclose, but
// with dual asynchronous opening.  Should lessen need for separate
// threads.  Not sure if it will be enough though.
//
//////////////////////////////////////////////////////////////////////
class ChunkQuery : public XrdPosixCallBack {
public:
    static const int MAX_ATTEMPTS = 3;
    enum WaitState {WRITE_QUEUE=100,
                    WRITE_OPEN, WRITE_WRITE,
                    READ_QUEUE,
		    READ_OPEN, READ_READ,
		    COMPLETE, CORRUPT, ABORTED};
    static char const* getWaitStateStr(WaitState s);

    class ReadCallable;
    class WriteCallable;
    friend class ReadCallable;
    friend class WriteCallable;

    virtual void Complete(int Result);
    explicit ChunkQuery(TransactionSpec const& t, int id,
			AsyncQueryManager* mgr);
    virtual ~ChunkQuery();

    void run();
    XrdTransResult const& results() const { return _result; }
    std::string getDesc() const;
    std::string const& getSavePath() const { return _spec.savePath; }
    int getSaveSize() const {
        if(_result.read >= 0) return _result.localWrite;
        else return -1;
    }
    boost::shared_ptr<PacketIter> getResultIter();
    // Attempt to squash this query's execution.  This implies that
    // nobody cares about this query's results anymore.
    void requestSquash();

private:
    void _sendQuery(int fd);
    void _readResults(int fd);
    void _readResultsDefer(int fd);
    void _notifyManager();
    bool _openForRead(std::string const& url);
    void _squashAtCallback(int result);
    void _unlinkResult(std::string const& url);

    int _id;
    TransactionSpec _spec;
    WaitState _state;
    XrdTransResult _result;
    boost::mutex _mutex;
    boost::shared_ptr<boost::mutex> _completeMutexP;
    std::string _hash;
    std::string _resultUrl;
    std::string _queryHostPort;
    boost::shared_ptr<PacketIter> _packetIter;
    AsyncQueryManager* _manager;
    bool _shouldSquash;
    Timer _writeOpenTimer;
    Timer _writeTimer;
    Timer _writeCloseTimer;
    Timer _readOpenTimer;
    Timer _readTimer;
    Timer _readCloseTimer;
    int _attempts;

};// class ChunkQuery

}}} // lsst::qserv::master namespace

#endif // LSST_QSERV_MASTER_CHUNKQUERY_H
