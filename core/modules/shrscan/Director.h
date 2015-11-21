// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

#ifndef LSST_QSERV_SHRSCAN_DIRECTOR_H
#define LSST_QSERV_SHRSCAN_DIRECTOR_H

// System headers
#include <memory>
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace shrscan {

//-----------------------------------------------------------------------------
//! @brief Describe a required table for a shared scan query.
//-----------------------------------------------------------------------------

class TableInfo {
public:

    std::string tableName;    //< Name of the table
    bool        lockIndex;    //< If true, the table's index is locked, if any.
    bool        lockData;     //< If true, the table's data  is locked

    //-----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param  tabName   is the name of the table.
    //! @param  lkIndex   whether or not to lock the table's index
    //! @param  lkData    whether or not to lock the table's data
    //-----------------------------------------------------------------------------

    TableInfo(std::string const &tabName, bool lkIndex=true, bool lkData=true)
             : tableName(tabName), lockIndex(lkIndex), lockData(lkData)
             {}
};

//-----------------------------------------------------------------------------
//! @brief The Query pure abstract class describes a shared scan query.
//-----------------------------------------------------------------------------

class Query {
public:

    //-----------------------------------------------------------------------------
    //! @brief The callback method to indicate a query has finished its run.
    //!
    //! The endQuery() method is called when the query completes or is canceled.
    //! This method is always called on a separate thread.
    //!
    //! @param  badChunks is reference to a vector of unprocessed chunks. This
    //!                   includes the chunk that was being processed if Run()
    //!                   returned false. The vector is deleted upon return.
    //! @return canceled  When true, the query was cancelled and badChunks will
    //!                   be a zero length vector. Otherwise, the query
    //!                   completed and badChunks does hold any unprocessed
    //!                   chunk numbers; which could be none.
    //-----------------------------------------------------------------------------

    virtual void endQuery(std::vector<int> const& badChunks,
                          bool canceled) = 0;

    //-----------------------------------------------------------------------------
    //! @brief The callack method to run a query on a chunk.
    //!
    //! The runQuery() method is invoked by the shared scan director to initiate
    //! the start of a query on a particular chunk. The method should only
    //! return when the query has been completed or cancelled.
    //!
    //! @param  chunkNum  is the number of the chunk to be processed.
    //!
    //! @return false     the query should be cancelled and endQuery() called.
    //! @return true      continue to the next chunk, if any.
    //-----------------------------------------------------------------------------

    virtual bool runQuery(int chunkNum) = 0;


                 Query() {}
    virtual     ~Query() {}
};

//-----------------------------------------------------------------------------
//! @brief The shared scan director.
//!
//! The Director is an abstract class the defines the interface to the
//! shared scan director that accepts, schedules, and dispatches queries.
//-----------------------------------------------------------------------------

class Director {
public:

    //-----------------------------------------------------------------------------
    //! @brief Add a query to the shared scan queue.
    //!
    //! @param  query  - Pointer to a Query object that describes the query.
    //! @param  tables - Reference to the tables required by the query.
    //-----------------------------------------------------------------------------

    virtual void  addQuery(std::shared_ptr<Query> const& query,
                           std::vector<TableInfo> const& tables
                          ) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Cancel a shared scan query.
    //!
    //! When a query is cancelled, the Query object's endQuery() method is
    //! called. If the query is executing its runQuery() method, cancellation
    //! occurs upon return.
    //!
    //! @param  query   - Pointer to a Query object that describes the query.
    //! @param  wait    - When true, does not return until the query has been
    //!                   actually cancelled (i.e. endQuery() method called).
    //!
    //! @return false: The query was not found.
    //! @return true:  The query was cancelled or was scheduled to be cancelled.
    //-----------------------------------------------------------------------------

    virtual bool  cancelQuery(Query const* query, bool wait=false) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Cancel all shared scan queries.
    //!
    //! @param  wait    - When true, does not return until all queries have been
    //!                   actually cancelled (i.e. endQuery() method called).
    //!
    //! This method effectively calls cancelQuery() on each query.
    //-----------------------------------------------------------------------------

    virtual void  cancelAll(bool wait=false) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Create a director and Initialize for processing.
    //!
    //! The create() method returns an initialized a shared scan director. This method
    //! may involve locking the base tables in memory which may be a lengthy
    //! processes.
    //!
    //! @param  info       - initialization information
    //! @param  baseTables - The tables that need to remain locked in memory.
    //! @param  chunkList  - The list of chunk numbers to be processed.
    //!
    //! @return !0: The pointer to the shared scan director.
    //! @return  0: A director could not be created (e.g. not all chunks found).
    //-----------------------------------------------------------------------------

    struct CreateInfo {
        unsigned long long maxMemory;  //< Maximum additional bytes to lock
        unsigned int       lockWindow; //< Number of chunks to lock at once
        unsigned int       max2Run;    //< Maximum parallel queries to run
        unsigned int       maxQWait;   //< Maximum milliseconds queue wait
        std::string        dbPath;     //< Path to directory where db resides

        CreateInfo() : maxMemory(0), lockWindow(5), max2Run(7),
                       maxQWait(60000) {}
    };

    static Director *create(CreateInfo const& info,
                            std::vector<TableInfo> const& baseTables,
                            std::vector<int> const& chunkList
                           );


    //-----------------------------------------------------------------------------
    //! @brief Obtain statistics about this shared scan director.
    //!
    //! @return The statistics.
    //-----------------------------------------------------------------------------

    struct Statistics {
        unsigned long long bytesLockMax;//< Maximum number of bytes to lock
        unsigned long long bytesLocked; //< Actual  number of bytes locked in memory
        unsigned long long msRunTotal;  //< Total milliseconds spent running
        unsigned long long msIdleTotal; //< Total milliseconds spent idling
        unsigned int       numChunks;   //< The number of chunks being handled
        unsigned int       maxCanRun;   //< Maximum number of parallel queries
        unsigned int       numQueued;   //< Number waiting to run
        unsigned int       numRunning;  //< Number actually running
        unsigned int       numSuspend;  //< Number currently suspended
        unsigned int       numCompleted;//< Number of queries that completed
        unsigned int       numErrors;   //< Number of queries that had errors
        unsigned int       numCancelled;//< Number of queries that were cancelled
    };

    virtual Statistics getStatistics() = 0;

    //-----------------------------------------------------------------------------
    //! @brief Obtain query status.
    //!
    //! @param  query   - Pointer to a Query object that describes the query.
    //!
    //! @return The query status.
    //-----------------------------------------------------------------------------

    struct Status {
        unsigned long long msInQ;     //< Milliseconds waiting to run (wall clock)
        unsigned long long msRunning; //< Milliseconds running so far (wall clock)
        unsigned long long msIdling;  //< Milliseconds in idle state  (wall clock)
        unsigned short     numChunks; //< Number of chunks processed
        unsigned short     badChunks; //< Number of chunks that were bad
        bool               isPresent; //< When true it exists (i.e. found)
        bool               isRunning; //< When true is actually running
        bool               isSuspend; //< When true is currently suspended
        bool               isCanceld; //< When true is cancelling
    };

    virtual Status getStatus(Query const* query) = 0;
    //-----------------------------------------------------------------------------
    //! @brief Resume a possibly suspended shared scan query.
    //!
    //! When a query is resumed it becomes eligible to run when sufficient
    //! resources become available.
    //!
    //! @param  query - Pointer to a Query object that describes the query.
    //!
    //! @return false: The query was not found.
    //! @return true:  The query was resumed.
    //-----------------------------------------------------------------------------

    virtual bool  resumeQuery(Query const* query) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Resume all suspended shared scan queries.
    //!
    //! This method effectively calls resumeQuery() on each suspended query.
    //-----------------------------------------------------------------------------

    virtual void  resumeAll() = 0;

    //-----------------------------------------------------------------------------
    //! @brief Suspend a shared scan query.
    //!
    //! When a running query is suspended, it is placed in the eligible to run
    //! queue in FIFO order. If the query is executing its runQuery() method,
    //! the suspension occurs upon return.
    //!
    //! @param  query   - Pointer to the Query object that describes the query.
    //! @param  unlock  - When true, any additional tables required by the query
    //!                   that were locked are unlocked. Otherwise, the tables
    //!                   remain locked which may prevent new queries to run.
    //!
    //! @return false: The query was not found.
    //! @return true:  The query was suspended or scheduled to be suspended.
    //-----------------------------------------------------------------------------

    virtual bool  suspendQuery(Query const* query, bool unlock=true) = 0;

    //-----------------------------------------------------------------------------
    //! @brief Suspend all shared scan queries.
    //!
    //! This method effectively calls suspendQuery() on each query (see above).
    //!
    //! @param  unlock  - When true, any additional tables required by the query
    //!                   that were locked are unlocked. Otherwise, the tables
    //!                   remain locked which may prevent new queries to run.
    //-----------------------------------------------------------------------------

    virtual void  suspendAll(bool unlock=true) = 0;

    //-----------------------------------------------------------------------------

    Director & operator=(const Director&) = delete;
    Director(const Director&) = delete;

                  Director() {}
    virtual      ~Director() {}
};

}}} // namespace lsst:qserv:shrscan
#endif
