// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
/// ReloadChunkListCommand.h
#ifndef LSST_QSERV_WCONTROL_RELOAD_CHUNK_LIST_COMMAND_H
#define LSST_QSERV_WCONTROL_RELOAD_CHUNK_LIST_COMMAND_H

// System headers
#include <memory>

// Qserv headers
#include "wbase/WorkerCommand.h"

// Forward declarations


namespace lsst {
namespace qserv {
namespace wcontrol {

/**
  * Class ReloadChunkListCommand reloads a list of chunks from the database
  */
class ReloadChunkListCommand
    :   public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    ReloadChunkListCommand& operator=(const ReloadChunkListCommand&) = delete;
    ReloadChunkListCommand(const ReloadChunkListCommand&) = delete;
    ReloadChunkListCommand() = delete;

    /**
     * The normal constructor of the class
     *
     * @param sendChannel - communication channel for reporting results
     */
    explicit ReloadChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel);

    /// The destructor
    virtual ~ReloadChunkListCommand();

    /**
     * Implement the corresponding method of the base class
     *
     * @see WorkerCommand::run()
     */
    void run () override;
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WCONTROL_RELOAD_CHUNK_LIST_COMMAND_H