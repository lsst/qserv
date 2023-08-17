// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_WPUBLISH_GET_DB_STATUS_COMMAND_H
#define LSST_QSERV_WPUBLISH_GET_DB_STATUS_COMMAND_H

// System headers
#include <memory>

// Qserv headers
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst::qserv::wbase {
class SendChannel;
}  // namespace lsst::qserv::wbase

// This header declarations
namespace lsst::qserv::wpublish {

/**
 * Class GetDbStatusCommand returns various info on the status of the database
 * service of the Qserv worker.
 */
class GetDbStatusCommand : public wbase::WorkerCommand {
public:
    /**
     * @param sendChannel The communication channel for reporting results.
     */
    GetDbStatusCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel);

    GetDbStatusCommand() = delete;
    GetDbStatusCommand& operator=(GetDbStatusCommand const&) = delete;
    GetDbStatusCommand(GetDbStatusCommand const&) = delete;

    virtual ~GetDbStatusCommand() override = default;

protected:
    virtual void run() override;
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_GET_DB_STATUS_COMMAND_H
