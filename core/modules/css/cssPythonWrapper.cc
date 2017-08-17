// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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

// Third-party headers
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

// LSST headers

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/KvInterface.h"
#include "css/MatchTableParams.h"
#include "css/NodeParams.h"
#include "css/PartTableParams.h"
#include "css/ScanTableParams.h"
#include "css/StripingParams.h"
#include "css/TableParams.h"

namespace py = pybind11;
using namespace pybind11::literals;

namespace lsst {
namespace qserv {
namespace css {

PYBIND11_PLUGIN(cssLib) {

    py::module mod("cssLib");

    py::class_<KvInterface, std::shared_ptr<KvInterface>>(mod, "KvInterface")
        .def("create", &KvInterface::create)
        .def("set", &KvInterface::set)
        .def("exists", &KvInterface::exists)
        .def("get", (std::string (KvInterface::*)(std::string const&))&KvInterface::get,
                "key"_a)
        .def("get", (std::string (KvInterface::*)(std::string const&, std::string const&))&KvInterface::get,
                "key"_a, "defaultValue"_a)
        .def("getMany", &KvInterface::getMany)
        .def("getChildren", &KvInterface::getChildren)
        .def("getChildrenValues", &KvInterface::getChildrenValues)
        .def("deleteKey", &KvInterface::deleteKey)
        .def("dumpKV", &KvInterface::dumpKV)
        ;

    py::class_<MatchTableParams>(mod, "MatchTableParams")
        .def(py::init<>())
        .def(py::init<std::string const&, std::string const&, std::string const&,
                std::string const&, std::string const&>())
        .def("isMatchTable", &MatchTableParams::isMatchTable)
        .def_readwrite("dirTable1", &MatchTableParams::dirTable1)
        .def_readwrite("dirColName1", &MatchTableParams::dirColName1)
        .def_readwrite("dirTable2", &MatchTableParams::dirTable2)
        .def_readwrite("dirColName2", &MatchTableParams::dirColName2)
        .def_readwrite("flagColName", &MatchTableParams::flagColName)
        ;

    py::class_<NodeParams>(mod, "NodeParams")
        .def(py::init<>())
        .def(py::init<std::string const&, std::string const&, int, std::string const&>())
        .def("isActive", &NodeParams::isActive)
        .def_readwrite("type", &NodeParams::type)
        .def_readwrite("host", &NodeParams::host)
        .def_readwrite("port", &NodeParams::port)
        .def_readwrite("state", &NodeParams::state)
        ;

    py::class_<PartTableParams>(mod, "PartTableParams")
        .def(py::init<>())
        .def(py::init<std::string const&, std::string const&, std::string const&,
                std::string const&, std::string const&, double, bool, bool>())
        .def("isPartitioned", &PartTableParams::isPartitioned)
        .def("isChunked", &PartTableParams::isChunked)
        .def("isSubChunked", &PartTableParams::isSubChunked)
        .def("chunkLevel", &PartTableParams::chunkLevel)
        .def("partitionCols", &PartTableParams::partitionCols)
        .def("secIndexColNames", &PartTableParams::secIndexColNames)
        .def_readwrite("dirDb", &PartTableParams::dirDb)
        .def_readwrite("dirTable", &PartTableParams::dirTable)
        .def_readwrite("dirColName", &PartTableParams::dirColName)
        .def_readwrite("latColName", &PartTableParams::latColName)
        .def_readwrite("lonColName", &PartTableParams::lonColName)
        .def_readwrite("overlap", &PartTableParams::overlap)
        .def_readwrite("partitioned", &PartTableParams::partitioned)
        .def_readwrite("subChunks", &PartTableParams::subChunks)
        ;

    py::class_<ScanTableParams>(mod, "ScanTableParams")
        .def(py::init<>())
        .def(py::init<bool, int>())
        .def_readwrite("lockInMem", &ScanTableParams::lockInMem)
        .def_readwrite("scanRating", &ScanTableParams::scanRating)
        ;

    py::class_<StripingParams>(mod, "StripingParams")
        .def(py::init<>())
        .def(py::init<int, int, int, double>())
        .def_readwrite("stripes", &StripingParams::stripes)
        .def_readwrite("subStripes", &StripingParams::subStripes)
        .def_readwrite("partitioningId", &StripingParams::partitioningId)
        .def_readwrite("overlap", &StripingParams::overlap)
        ;

    py::class_<TableParams>(mod, "TableParams")
        .def(py::init<>())
        .def_readwrite("match", &TableParams::match)
        .def_readwrite("partitioning", &TableParams::partitioning)
        .def_readwrite("sharedScan", &TableParams::sharedScan)
        ;

    py::class_<CssAccess, std::shared_ptr<CssAccess>>(mod, "CssAccess")
        .def_static("createFromData", &CssAccess::createFromData,
                "data"_a, "emptyChunkPath"_a, "readOnly"_a = false)
        .def_static("createFromConfig", &CssAccess::createFromConfig,
                "config"_a, "emptyChunkPath"_a, "readOnly"_a = false)
        .def_static("cssVersion", &CssAccess::cssVersion)
        .def("getDbNames", &CssAccess::getDbNames)
        .def("getDbStatus", &CssAccess::getDbStatus)
        .def("setDbStatus", &CssAccess::setDbStatus)
        .def("containsDb", &CssAccess::containsDb)
        .def("getDbStriping", &CssAccess::getDbStriping)
        .def("createDb", &CssAccess::createDb)
        .def("createDbLike", &CssAccess::createDbLike)
        .def("dropDb", &CssAccess::dropDb)
        .def("getTableNames", &CssAccess::getTableNames,
                "dbName"_a, "readyOnly"_a=true)
        .def("getTableStatus", &CssAccess::getTableStatus)
        .def("setTableStatus", &CssAccess::setTableStatus)
        .def("containsTable", &CssAccess::containsTable,
                "dbName"_a, "tableName"_a, "readyOnly"_a=true)
        .def("getMatchTableParams", &CssAccess::getMatchTableParams)
        .def("getPartTableParams", &CssAccess::getPartTableParams)
        .def("getScanTableParams", &CssAccess::getScanTableParams)
        .def("getTableParams", &CssAccess::getTableParams)
        .def("createTable", &CssAccess::createTable)
        .def("createMatchTable", &CssAccess::createMatchTable)
        .def("dropTable", &CssAccess::dropTable)
        .def("getNodeNames", &CssAccess::getNodeNames)
        .def("getNodeParams", &CssAccess::getNodeParams)
        .def("getAllNodeParams", &CssAccess::getAllNodeParams)
        .def("addNode", &CssAccess::addNode)
        .def("setNodeState", &CssAccess::setNodeState)
        .def("deleteNode", &CssAccess::deleteNode)
        .def("addChunk", &CssAccess::addChunk)
        .def("deleteChunk", &CssAccess::deleteChunk)
        .def("getChunks", &CssAccess::getChunks)
        // getEmptyChunks is intentionally skipped, not used by Python code
        //.def("getEmptyChunks", &CssAccess::getEmptyChunks)
        .def("getKvI", &CssAccess::getKvI)
        ;

    static py::exception<CssError> CssError(mod, "CssError");
    py::register_exception<NoSuchDb>(mod, "NoSuchDb", CssError.ptr());
    py::register_exception<NoSuchKey>(mod, "NoSuchKey", CssError.ptr());
    py::register_exception<NoSuchTable>(mod, "NoSuchTable", CssError.ptr());
    py::register_exception<TableExists>(mod, "TableExists", CssError.ptr());
    py::register_exception<AuthError>(mod, "AuthError", CssError.ptr());
    py::register_exception<ConnError>(mod, "ConnError", CssError.ptr());
    py::register_exception<KeyExistsError>(mod, "KeyExistsError", CssError.ptr());
    py::register_exception<KeyValueError>(mod, "KeyValueError", CssError.ptr());
    py::register_exception<BadAllocError>(mod, "BadAllocError", CssError.ptr());
    py::register_exception<VersionMissingError>(mod, "VersionMissingError", CssError.ptr());
    py::register_exception<VersionMismatchError>(mod, "VersionMismatchError", CssError.ptr());
    py::register_exception<ReadonlyCss>(mod, "ReadonlyCss", CssError.ptr());
    py::register_exception<NoSuchNode>(mod, "NoSuchNode", CssError.ptr());
    py::register_exception<NodeExists>(mod, "NodeExists", CssError.ptr());
    py::register_exception<NodeInUse>(mod, "NodeInUse", CssError.ptr());
    py::register_exception<ConfigError>(mod, "ConfigError", CssError.ptr());

    // module-level constants
    mod.attr("VERSION_KEY") = VERSION_KEY;
    mod.attr("VERSION") = VERSION;
    mod.attr("VERSION_STR") = VERSION_STR;
    mod.attr("KEY_STATUS_IGNORE") = KEY_STATUS_IGNORE;
    mod.attr("KEY_STATUS_READY") = KEY_STATUS_READY;
    mod.attr("KEY_STATUS_CREATE_PFX") = KEY_STATUS_CREATE_PFX;
    mod.attr("KEY_STATUS_DROP_PFX") = KEY_STATUS_DROP_PFX;
    mod.attr("KEY_STATUS_FAILED_PFX") = KEY_STATUS_FAILED_PFX;
    mod.attr("NODE_STATE_ACTIVE") = NODE_STATE_ACTIVE;
    mod.attr("NODE_STATE_INACTIVE") = NODE_STATE_INACTIVE;

    return mod.ptr();

}

}}} // namespace lsst::qserv::css
