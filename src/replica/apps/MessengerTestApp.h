/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_MESSENGERTESTAPP_H
#define LSST_QSERV_REPLICA_MESSENGERTESTAPP_H

// System headers
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class MessengerTestApp implements a tool which tests the Messenger Network
 * w/o leaving side effects on the workers. The tool will be sending and tracking
 * requests of class EchoRequest.
 *
 * @see class EchoRequest
 */
class MessengerTestApp : public Application {
public:
    typedef std::shared_ptr<MessengerTestApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    MessengerTestApp() = delete;
    MessengerTestApp(MessengerTestApp const&) = delete;
    MessengerTestApp& operator=(MessengerTestApp const&) = delete;

    virtual ~MessengerTestApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see MessengerTestApp::create()
    MessengerTestApp(int argc, char* argv[]);

    /**
     * Log an event regarding a request for futher reporting by method _reportEvents;
     * @param lock A lock on mutex _mtx to be held before callin the method.
     * @param timeMs A timestamp of the event to be reported.
     * @param event An event to be reported.
     */
    void _logEvent(std::unique_lock<std::mutex> const& lock, uint64_t timeMs, std::string const& event);

    /// Report events (if enabled).
    /// @param lock A lock on mutex _mtx to be held before callin the method.
    void _reportEvents(std::unique_lock<std::mutex> const& lock);

    // Command line parameters of the application

    /// The name of a worker to be used during the testing.
    std::string _workerName;

    /// The data payload to be sent to the worker and expected to be back.
    std::string _data = "ECHO";

    /// The 'processing' time (seconds) of requests by the worker's threads.
    /// This interval doesn't include any latencies for delivering requests to
    /// the threads and retreiving results back. If a value of the parameter
    /// is set to 0 then requests will be instantly answered by the worker
    /// w/o putting them into a queue for further processing by the workers'
    /// threads. Zero value is also good for testing the performance of the protocol.
    uint64_t _proccesingTimeSec = 0;

    /// Priority level of requests.
    int _priority = 0;

    /// The request expiration interval (seconds). The parameter determines
    /// the maximum "life expectancy" of requests after they're submitted and before
    /// they finish, failed or expired. The default value of 0 will result in fetching
    /// a value of the parameter from the Replication System's configuration.
    unsigned int _requestExpirationIvalSec = 0;

    /// The total number of requests to be submitted to the worker. A value of
    /// the parameter must be strictly greater than 0.
    int _totalRequests = 1;

    /// The number of in-flight requests not to exceed at any given moment of time.
    /// This parameter is used for flow control, and to prevent the application
    /// from consuming too much memory. A value of the parameter should not be
    /// less than the total number of requests to be submitted to the worker.
    int _maxActiveRequests = 1;

    /// The interval for reporting events. If a value of the parameter
    /// is 0 then events won't be reported.
    unsigned int _eventsReportIvalSec = 1;

    /// Enable extended reporting on sending requests and analysing responses.
    bool _reportRequestEvents = false;

    // Synchronization primitives for tracking active requests
    // and updating statistics.

    std::mutex _mtx;
    std::condition_variable _onNumActiveCv;

    int _numActive = 0;    // The current number of the active (in-flight) requests.
    int _numFinished = 0;  // Total number of finished requests regardless of the state.
    int _numSuccess = 0;
    int _numExpired = 0;
    int _numFailed = 0;

    // Synchronized log of events is used for accumulating start and finish events
    // of requests. Entries are insert into the log by method _logEvent(). Entries are
    // reported to the standard output stream by method _reportEvents(). The reported
    // entries are deleted by the later method after reporting.
    std::list<std::pair<uint64_t, std::string>> _eventLog;

    uint64_t _prevEventsReportMs = 0;  ///< The last time the event report was made
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_MESSENGERTESTAPP_H */
