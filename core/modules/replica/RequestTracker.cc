/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

// Class header
#include "replica/RequestTracker.h"

// System headers

// Qserv headers
#include "replica/BlockPost.h"
#include "replica/Controller.h"

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////
//          RequestTrackerBase           //
///////////////////////////////////////////

RequestTrackerBase::RequestTrackerBase (std::ostream& os,
                                        bool          progressReport,
                                        bool          errorReport)
    :   _numLaunched (0),
        _numFinished (0),
        _numSuccess  (0),
        _os(os),
        _progressReport(progressReport),
        _errorReport   (errorReport) {
}

void RequestTrackerBase::track () const {

    // Wait before all request are finished. Then analyze results
    // and print a report on failed requests (if any)

    replica::BlockPost blockPost (100, 200);
    while (_numFinished < _numLaunched) {
        blockPost.wait();
        if (_progressReport) {
            _os << "RequestTracker::track()  "
                << "launched: " << _numLaunched << ", "
                << "finished: " << _numFinished << ", "
                << "success: "  << _numSuccess
                << std::endl;
        }
    }
    if (_progressReport) {
        _os << "RequestTracker::track()  "
            << "launched: " << _numLaunched << ", "
            << "finished: " << _numFinished << ", "
            << "success: "  << _numSuccess
            << std::endl;
    }
    if (_errorReport and (_numLaunched - _numSuccess)) {
        printErrorReport (_os);
    }
}

void RequestTrackerBase::cancel (bool propagateToServers) {
    
    auto onFinish     = nullptr;
    bool keepTracking = false;

    for (auto const& ptr: getRequests()) {

        if (ptr->state() != Request::State::FINISHED) {
            ptr->cancel();

            if (propagateToServers) {
                if (auto controller = ptr->controller()) {

                    if (ptr->type()  == "REPLICA_CREATE") {
                        controller->stopReplication (
                            ptr->worker(),
                            ptr->id(),
                            onFinish,
                            keepTracking
                        );
                    } else if (ptr->type()  == "REPLICA_DELETE") {
                        controller->stopReplicaDelete (
                            ptr->worker(),
                            ptr->id(),
                            onFinish,
                            keepTracking
                        );
                    } else if (ptr->type()  == "REPLICA_FIND") {
                        controller->stopReplicaFind (
                            ptr->worker(),
                            ptr->id(),
                            onFinish,
                            keepTracking
                        );
                    } else if (ptr->type()  == "REPLICA_FIND_ALL") {
                        controller->stopReplicaFindAll (
                            ptr->worker(),
                            ptr->id(),
                            onFinish,
                            keepTracking
                        );
                    }
                }
            }
        }
    }
}

void RequestTrackerBase::reset () {
    size_t const numOutstanding = RequestTrackerBase::_numLaunched -
                                  RequestTrackerBase::_numFinished;
    if (numOutstanding) {
        throw std::logic_error (
                "RequestTrackerBase::reset  the operation is not allowed due to " +
                std::to_string(numOutstanding) + " outstanding requests");
    }
    resetImpl();

    RequestTrackerBase::_numLaunched = 0;
    RequestTrackerBase::_numFinished = 0;
    RequestTrackerBase::_numSuccess  = 0;
}

//////////////////////////////////////////
//          AnyRequestTracker           //
//////////////////////////////////////////

AnyRequestTracker::AnyRequestTracker (std::ostream& os,
                                      bool progressReport,
                                      bool errorReport)
    :   RequestTrackerBase (os,
                            progressReport,
                            errorReport) {
}

void AnyRequestTracker::onFinish (Request::pointer const& ptr) {
    RequestTrackerBase::_numFinished++;
    if (ptr->extendedState() == Request::ExtendedState::SUCCESS) {
        RequestTrackerBase::_numSuccess++;
    }
}

void AnyRequestTracker::add (Request::pointer const& ptr) {
    RequestTrackerBase::_numLaunched++;
    requests.push_back(ptr);
}

void AnyRequestTracker::printErrorReport (std::ostream& os) const {
    replica::reportRequestState (requests, os);
}

std::list<Request::pointer> AnyRequestTracker::getRequests () const {
    return requests;
}

void AnyRequestTracker::resetImpl () {
    requests.clear();
}

}}} // namespace lsst::qserv::replica