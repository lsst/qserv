/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

// System headers
#include <iostream>
#include <stdexcept>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "partition/ChunkIndex.h"
#include "partition/ConfigStore.h"
#include "partition/Exceptions.h"
#include "partition/PartitionMatchesTool.h"

int main(int argc, char const* const* argv) {
    try {
        lsst::partition::PartitionMatchesTool partitioner(nlohmann::json::object(), argc, argv);
        if (partitioner.config->flag("verbose")) {
            partitioner.chunkIndex->write(std::cout, 0);
            std::cout << std::endl;
        }
    } catch (lsst::partition::ExitOnHelp const& ex) {
        std::cout << ex.what() << std::endl;
    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
