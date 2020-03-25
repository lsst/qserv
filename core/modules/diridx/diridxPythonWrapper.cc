#include <pybind11/pybind11.h>
#include <pybind11/functional.h>

#include "diridx/DirIdxRedisClient.h"
#include "diridx/DirIdxRedisClientContext.h"


namespace py = pybind11;

int add(int i, int j) {
    return i + j;
}

namespace lsst {
namespace qserv {
namespace diridx {


PYBIND11_MODULE(diridxLib, m) {

    m.def("add", &add, "A function which adds two numbers");

    py::class_<DirIdxRedisClientContext>(m, "DirIdxRedisClientContext")
        .def(py::init<std::string const&>())
        .def("dirIdxRedisClient", &DirIdxRedisClientContext::dirIdxRedisClient, py::return_value_policy::reference_internal);

    py::class_<DirIdxRedisClient> dirIdxRedisClient(m, "DirIdxRedisClient");
    dirIdxRedisClient.def("set", &DirIdxRedisClient::set)
        .def("get", (ChunkData (DirIdxRedisClient::*)(long long unsigned int)) &DirIdxRedisClient::get)
        .def("get", (ChunkData (DirIdxRedisClient::*)(std::string id)) &DirIdxRedisClient::get);

    py::enum_<DirIdxRedisClient::Err>(dirIdxRedisClient, "DirIdxRedisClient")
        .value("SUCCESS", DirIdxRedisClient::Err::SUCCESS)
        .value("FAIL", DirIdxRedisClient::Err::FAIL);

    py::class_<ChunkData>(m, "ChunkData")
        .def("chunkId", &ChunkData::chunkId)
        .def("subChunkId", &ChunkData::subChunkId)
        .def("__repr__", &ChunkData::toString);
}


}}}
