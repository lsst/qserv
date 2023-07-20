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

// Class header
#include "wbase/SendChannelShared.h"

// Qserv headers
#include "proto/ProtoHeaderWrap.h"
#include "wbase/Task.h"
#include "wpublish/QueriesAndChunks.h"
#include "util/MultiError.h"
#include "util/Timer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.SendChannelShared");
}

namespace lsst::qserv::wbase {

SendChannelShared::Ptr SendChannelShared::create(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                                 qmeta::CzarId czarId) {
    return shared_ptr<SendChannelShared>(new SendChannelShared(sendChannel, transmitMgr, czarId));
}

SendChannelShared::SendChannelShared(shared_ptr<wbase::SendChannel> const& sendChannel,
                                     shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
                                     qmeta::CzarId czarId)
        : ChannelShared(sendChannel, transmitMgr, czarId) {}

bool SendChannelShared::buildAndTransmitResult(MYSQL_RES* mResult, Task::Ptr const& task,
                                               util::MultiError& multiErr, atomic<bool>& cancelled) {
    util::Timer transmitT;
    transmitT.start();
    double bufferFillSecs = 0.0;

    // 'cancelled' is passed as a reference so that if its value is
    // changed externally, it will break the while loop below.
    // Wait until the transmit Manager says it is ok to send data to the czar.
    auto qId = task->getQueryId();
    bool scanInteractive = task->getScanInteractive();
    waitTransmitLock(scanInteractive, qId);

    // Lock the transmit mutex until this is done.
    lock_guard<mutex> const tMtxLock(tMtx);

    // Initialize transmitData, if needed.
    initTransmit(tMtxLock, *task);

    bool erred = false;
    size_t tSize = 0;

    int bytesTransmitted = 0;
    int rowsTransmitted = 0;

    // If fillRows returns false, transmitData is full and needs to be transmitted
    // fillRows returns true when there are no more rows in mResult to add.
    // tSize is set by fillRows.
    bool more = true;
    while (more && !cancelled) {
        util::Timer bufferFillT;
        bufferFillT.start();
        more = !transmitData->fillRows(mResult, tSize);
        bytesTransmitted += transmitData->getResultSize();
        rowsTransmitted += transmitData->getResultRowCount();
        transmitData->buildDataMsg(*task, multiErr);
        bufferFillT.stop();
        bufferFillSecs += bufferFillT.getElapsed();
        LOGS(_log, LOG_LVL_TRACE,
             "buildAndTransmitResult() more=" << more << " " << task->getIdStr() << " seq=" << task->getTSeq()
                                              << dumpTransmit(tMtxLock));

        // This will become true only if this is the last task sending its last transmit.
        // prepTransmit will add the message to the queue and may try to transmit it now.
        bool lastIn = false;
        if (more) {
            if (!prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                LOGS(_log, LOG_LVL_ERROR, "Could not transmit intermediate results.");
                erred = true;
                break;
            }
        } else {
            lastIn = transmitTaskLast();
            // If 'lastIn', this is the last transmit and it needs to be added.
            // Otherwise, just append the next query result rows to the existing transmitData
            // and send it later.
            if (lastIn && !prepTransmit(tMtxLock, task, cancelled, lastIn)) {
                LOGS(_log, LOG_LVL_ERROR, "Could not transmit intermediate results.");
                erred = true;
                break;
            }
        }
    }
    transmitT.stop();
    double timeSeconds = transmitT.getElapsed();
    auto qStats = task->getQueryStats();
    if (qStats == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, "No statistics for " << task->getIdStr());
    } else {
        qStats->addTaskTransmit(timeSeconds, bytesTransmitted, rowsTransmitted, bufferFillSecs);
        LOGS(_log, LOG_LVL_TRACE,
             "TaskTransmit time=" << timeSeconds << " bufferFillSecs=" << bufferFillSecs);
    }
    return erred;
}

}  // namespace lsst::qserv::wbase
