#include <iostream>

#include <boost/make_shared.hpp>

#include "lsst/qserv/master/AsyncQueryManager.h"
#include "lsst/qserv/master/ChunkQuery.h"
#include "lsst/qserv/master/TableMerger.h"

// Namespace modifiers
using boost::make_shared;
namespace qMaster = lsst::qserv::master;


// Local Helpers --------------------------------------------------
namespace { 

// Doctors the query path to specify the async path.
// Modifies the string in-place.
void doctorQueryPath(std::string& path) {
    std::string::size_type pos;
    std::string before("/query/");
    std::string after("/query2/");

    pos = path.find(before);
    if(pos != std::string::npos) {
	path.replace(pos, before.size(), after);
    } // Otherwise, don't doctor.
}

}

////////////////////////////////////////////////////////////
// AsyncQueryManager nested classes
////////////////////////////////////////////////////////////

class qMaster::AsyncQueryManager::printQueryMapValue {
public:
    printQueryMapValue(std::ostream& os_) : os(os_) {}
    void operator()(QueryMap::value_type const& qv) {
        os << "Query with id=" << qv.first;
        os << ": " << qv.second.first->getDesc() 
           << ", " << qv.second.second << std::endl;
    }
    std::ostream& os;
};

class qMaster::AsyncQueryManager::squashQuery {
public:
    squashQuery() {}
    void operator()(QueryMap::value_type const& qv) {
        boost::shared_ptr<ChunkQuery> cq = qv.second.first;
        cq->requestSquash();
    }
};

////////////////////////////////////////////////////////////
// AsyncQueryManager
////////////////////////////////////////////////////////////
int qMaster::AsyncQueryManager::add(TransactionSpec const& t, 
				    std::string const& resultName) {
    int id = t.chunkId;
    // Use chunkId as id, and assume that it will be unique for the 
    // AsyncQueryManager instance.
    if(id == -1) {
        id = _getNextId();
    }
    if(t.isNull() || _isExecFaulty) { 
        // If empty spec or fault already detected, refuse to run.
        return -1; 
    }
    TransactionSpec ts(t);


    doctorQueryPath(ts.path);
    QuerySpec qs(boost::make_shared<ChunkQuery>(ts, id, this),
		 resultName);
    {
	boost::lock_guard<boost::mutex> lock(_queriesMutex);
	_queries[id] = qs;
        ++_queryCount;
    }
    std::cout << "Added query id=" << id << " url=" << ts.path 
	      << " with save " << ts.savePath << "\n";

    qs.first->run();
}

void qMaster::AsyncQueryManager::finalizeQuery(int id, 
					       XrdTransResult r,
                                               bool aborted) {
    /// Finalize a query.
    /// Note that all parameters should be copies and not const references.
    /// We delete the ChunkQuery (the caller) here, so a ref would be invalid.
    std::string dumpFile;
    std::string tableName;
    // std::cout << "finalizing. read=" << r.read << " and status is "
    //           << (aborted ? "ABORTED" : "okay") << std::endl;

    if((!aborted) && (r.read >= 0)) {
	{ // Lock scope for reading
	    boost::lock_guard<boost::mutex> lock(_queriesMutex);
	    QuerySpec& s = _queries[id];
	    dumpFile = s.first->getSavePath();
	    tableName = s.second;
	    s.first.reset(); // clear out chunkquery.
        } // Lock-free merge
        if(r.localWrite > 0) {
            bool mergeResult = _merger->merge(dumpFile, tableName);
             // std::cout << "Merge of " << dumpFile << " into "
             //           << tableName 
             //           << (mergeResult ? " OK----" : " FAIL====") 
             //           << std::endl;
        }
        { // Lock again to erase.
	    boost::lock_guard<boost::mutex> lock(_queriesMutex);
            _queries.erase(id); // Don't need it anymore
	} 
    } // end if 
    else { 
        {
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            _queries.erase(id);
        }
        if(!aborted) {
            _isExecFaulty = true;
            _squashExecution();
            std::cout << " Skipped merge (read failed for id=" 
                      << id << ")" << std::endl;
        } 
    }
    {
	boost::lock_guard<boost::mutex> lock(_resultsMutex);
	_results.push_back(Result(id,r));
        if(aborted) ++_squashCount; // Borrow result mutex to protect counter.
    }
}

void qMaster::AsyncQueryManager::joinEverything() {
    boost::unique_lock<boost::mutex> lock(_queriesMutex);
    int lastCount = -1;
    int count;
    //_printState(std::cout);
    while(!_queries.empty()) { // FIXME: Should make this condition-var based.
        count = _queries.size();
        if(count != lastCount) {
            std::cout << "Still " << count
                      << " in flight." << std::endl;
            count = lastCount;
        }
        lock.unlock();
	sleep(1); 
        lock.lock();
    }
    {
	boost::lock_guard<boost::mutex> lock(_resultsMutex);
        while(_results.size() < _queryCount) {
            std::cout << "Waiting for " << (_queryCount - _results.size())
                      << " results to report" << std::endl;
        }
    }
    _merger->finalize();
}

void qMaster::AsyncQueryManager::configureMerger(TableMergerConfig const& c) {
    _merger = boost::make_shared<TableMerger>(c);
}

std::string qMaster::AsyncQueryManager::getMergeResultName() const {
    if(_merger.get()) {
	return _merger->getTargetTable();
    }
    return std::string();
}


void qMaster::AsyncQueryManager::_printState(std::ostream& os) {
    typedef std::map<int, boost::shared_ptr<ChunkQuery> > QueryMap;
    std::for_each(_queries.begin(), _queries.end(), printQueryMapValue(os));

}

void qMaster::AsyncQueryManager::_squashExecution() {
    // Halt new query dispatches and cancel the ones in flight.
    // This attempts to save on resources and latency, once a query
    // fault is detected.
    boost::unique_lock<boost::mutex> lock(_queriesMutex);
    typedef std::map<int, boost::shared_ptr<ChunkQuery> > QueryMap;
    std::for_each(_queries.begin(), _queries.end(), squashQuery());

}
