// -*- LSST-C++ -*-

// Class header
#include "proto/WorkerResponse.h"

// System headers
#include <atomic>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers

namespace {
LOG_LOGGER _log = LOG_GET("lsst.proto.WorkerResponse");
std::atomic<int> clWorkerResponseInstCount{0};
}

namespace lsst {
namespace qserv {
namespace proto {

WorkerResponse::WorkerResponse() {
    LOGS(_log,LOG_LVL_DEBUG, "&&& clWorkerResponseInstCount=" << ++clWorkerResponseInstCount);
}

WorkerResponse::~WorkerResponse() {
    LOGS(_log,LOG_LVL_DEBUG, "~&&& clWorkerResponseInstCount=" << --clWorkerResponseInstCount);
}

}}} // namespace lsst::qserv::proto

