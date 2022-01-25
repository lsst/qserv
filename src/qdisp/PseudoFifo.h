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


#ifndef LSST_QSERV_QDISP_PSEUDOFIFO_H
#define LSST_QSERV_QDISP_PSEUDOFIFO_H

// System headers
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

namespace lsst {
namespace qserv {
namespace qdisp {

/// This class only allows the last _maxRunningCount elements
/// to run at any given time. The _runningCount is decremented
/// by the Element destructor, so care needs to be taken that
/// the element is destroyed at an appropriate time.
/// PseudoFifo must outlive all of its Elements.
///
/// When the worker is out of resources, the older QueryRequests must be answered first
/// to free up resources. The newest QueryRequests may require the worker to allocate
/// resources before an answer can be sent. If this happens, worker deadlock is
/// highly likely. This class tries to prevent that from happening.
class PseudoFifo {
public:
    using Ptr = std::shared_ptr<PseudoFifo>;

    class Element {
    public:
        using Ptr = std::shared_ptr<Element>;
        Element() = delete;
        Element(PseudoFifo& pseudoF) : sid(seq++), _pseudoF(pseudoF) {}

        ~Element() {
            if (_go) {
                _pseudoF._finished();
            }
            // else it never started running so nothing to decrement.
        }

        /// Wait until go() is called.
        void wait();
        void go();

        static uint32_t seq; ///< Static source for sequence ids.
        uint32_t const sid;  ///< Sequence id.

    private:
        bool _go = false;
        std::mutex _eMtx;
        std::condition_variable _eCv;
        PseudoFifo& _pseudoF;
    };


    PseudoFifo() = delete;
    PseudoFifo(int maxRunningCount) : _maxRunningCount(maxRunningCount) {}
    PseudoFifo(PseudoFifo const&) = delete;
    PseudoFifo& operator=(PseudoFifo const&) = delete;

    /// Put this element on the queue. It will need to wait until fewer
    /// than _maxRunningCount items are running and it has reached the
    /// front of the queue, before it can go.
    /// The returned pointer should not be reset until this element
    /// has finished running.
    Element::Ptr queueAndWait();

private:
    /// _qMtx must be held before calling this function.
    /// Pop and run elements until the maximum number of elements
    /// are running.
    void _runSomeElements();

    /// This is called by the Element destructor.
    void _finished() {
        std::unique_lock<std::mutex> qLock(_qMtx);
        --_runningCount;
        _runSomeElements();
    }

    int _runningCount = 0;
    int _maxRunningCount;

    std::deque<Element::Ptr> _queue;
    std::mutex _qMtx;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_PSEUDOFIFO_H
