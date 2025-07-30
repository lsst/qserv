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
#ifndef LSST_QSERV_CZAR_CZARTHREADS_H
#define LSST_QSERV_CZAR_CZARTHREADS_H

// System headers
#include <memory>

// Qserv headers
#include "qmeta/types.h"

// Forward declarations

namespace lsst::qserv::cconfig {
class CzarConfig;
}  // namespace lsst::qserv::cconfig

namespace lsst::qserv::qmeta {
class QMeta;
}  // namespace lsst::qserv::qmeta

// This header declarations

namespace lsst::qserv::czar {

/**
 * Start a detached thread that will periodically update Czar info
 * in the Replication System's Registry.
 *
 * @note The thread will terminate the process if the registraton request
 * to the Registry was explicitly denied by the service. This means
 * the application may be misconfigured. Transient communication errors
 * when attempting to connect or send requests to the Registry will be posted
 * into the log stream and ignored.
 *
 * @param czarConfig A pointer to the Czar configuration service.
 */
void startRegistryUpdate(std::shared_ptr<cconfig::CzarConfig> czarConfig);

/**
 * Start a detached thread that will periodically check for a presence of
 * the outdated tables in the result database. The tables will be removed.
 *
 * @note The age of the tables is determined by the parameter "oldestResultKeptDays"
 * in the Czar's configuration service. The tables that are older than the specified
 * number of days will be removed. Note that this operation doesn't discrimitate
 * between the tables that are may still be in progress and those that are not.
 * An assumption is made that the table age threshold set in the parameter
 * "oldestResultKeptDays" is long enough to allow the Czar to finish all the queries.
 * The reasonable value for this parameter is 1 day.
 *
 * @param czarConfig A pointer to the Czar configuration service.
 */
void startGarbageCollect(std::shared_ptr<cconfig::CzarConfig> czarConfig);

/**
 * Start a detached thread that will periodically check a status of the completed
 * asyncronous queries. The result and message tables of the outdated queries will
 * be removed. This mechanism is meant to pick up the garbage left by the unclaimed
 * result sets. This special case is needed due to the migration of the Qserv query API
 * to the explicit result set removal. The old API used to remove the result sets
 * automatically when the result set was no longer needed. The new API requires
 * the user to remove the result set explicitly.
 *
 * @note The age of the queries is determined by both parameters "oldestResultKeptDays"
 * and "oldestAsyncResultKeptSeconds" in the Czar's configuration service. The tables of
 * queries whose completion time is newer than the one defined by "oldestResultKeptDays"
 * and older than the specified number of seconds defined in "oldestAsyncResultKeptSeconds"
 * will be removed. The reasonable value for this parameter is 1 hour.
 *
 * @param czarConfig A pointer to the Czar configuration service.
 */
void startGarbageCollectAsync(std::shared_ptr<cconfig::CzarConfig> czarConfig);

/**
 * Start a detached thread that will periodically check a status of the in-progress
 * queries. The entries of the queries that are no longer in progress will be removed.
 *
 * @note The period of the tests is determined by a value of the configuraton
 * parameters "secondsBetweenInProgressUpdates" in the Czar's configuration service.
 *
 * @param czarConfig A pointer to the Czar configuration service.
 * @param czarId The identifier of the Czar instance.
 * @param queryMetadata A pointer to the QMeta service.
 */
void startGarbageCollectInProgress(std::shared_ptr<cconfig::CzarConfig> czarConfig, qmeta::CzarId czarId,
                                   std::shared_ptr<qmeta::QMeta> queryMetadata);

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_CZARTHREADS_H
