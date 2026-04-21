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

// System headers
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "replica/worker/WorkerHttpRequest.h"

// Boost unit test header
#define BOOST_TEST_MODULE WorkerHttpRequest
#include <boost/test/unit_test.hpp>

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

class TestWorkerHttpRequest : public WorkerHttpRequest {
public:
    explicit TestWorkerHttpRequest(json const& req) : WorkerHttpRequest(req) {}
    ~TestWorkerHttpRequest() override = default;

    bool execute() override { return true; }
    void getResult(json& result) const override { result = json::object(); }

    using WorkerHttpRequest::hasParam;
    using WorkerHttpRequest::optParamBool;
    using WorkerHttpRequest::optParamDouble;
    using WorkerHttpRequest::optParamInt32;
    using WorkerHttpRequest::optParamString;
    using WorkerHttpRequest::optParamStringVec;
    using WorkerHttpRequest::optParamUInt16;
    using WorkerHttpRequest::optParamUInt32;
    using WorkerHttpRequest::optParamUInt64;
    using WorkerHttpRequest::optParamUInt64Vec;
    using WorkerHttpRequest::reqParamBool;
    using WorkerHttpRequest::reqParamDouble;
    using WorkerHttpRequest::reqParamInt32;
    using WorkerHttpRequest::reqParamObj;
    using WorkerHttpRequest::reqParamString;
    using WorkerHttpRequest::reqParamStringVec;
    using WorkerHttpRequest::reqParamUInt16;
    using WorkerHttpRequest::reqParamUInt32;
    using WorkerHttpRequest::reqParamUInt64;
    using WorkerHttpRequest::reqParamUInt64Vec;
    using WorkerHttpRequest::reqParamVec;
};

}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(WorkerHttpRequestParsingSuccess) {
    uint64_t const u64 = std::numeric_limits<uint64_t>::max();

    TestWorkerHttpRequest const request({
            {"string", "abc"},
            {"bool1", true},
            {"bool2", false},
            {"bool3", 1},
            {"bool4", 0U},
            {"u16", 65535U},
            {"u32", 4294967295ULL},
            {"i32", -2147483648LL},
            {"u64", u64},
            {"dbl", 1.25},
            {"strVec", json::array({"one", "two"})},
            {"u64Vec", json::array({5ULL, 7ULL})},
            {"arr", json::array({1, 2, 3})},
            {"obj", json::object({{"k", "v"}})},
    });

    BOOST_CHECK(request.hasParam("string"));
    BOOST_CHECK(!request.hasParam("missing"));

    BOOST_CHECK_EQUAL(request.reqParamString("string"), "abc");
    BOOST_CHECK_EQUAL(request.optParamString("missing", "default"), "default");

    BOOST_CHECK(request.reqParamBool("bool1"));
    BOOST_CHECK(!request.reqParamBool("bool2"));
    BOOST_CHECK(request.reqParamBool("bool3"));
    BOOST_CHECK(!request.reqParamBool("bool4"));
    BOOST_CHECK(request.optParamBool("missing", true));

    BOOST_CHECK_EQUAL(request.reqParamUInt16("u16"), static_cast<uint16_t>(65535U));
    BOOST_CHECK_EQUAL(request.optParamUInt16("missing", static_cast<uint16_t>(9U)),
                      static_cast<uint16_t>(9U));

    BOOST_CHECK_EQUAL(request.reqParamUInt32("u32"), static_cast<uint32_t>(4294967295ULL));
    BOOST_CHECK_EQUAL(request.optParamUInt32("missing", 37U), 37U);

    BOOST_CHECK_EQUAL(request.reqParamInt32("i32"), static_cast<int32_t>(-2147483648LL));
    BOOST_CHECK_EQUAL(request.optParamInt32("missing", -9), -9);

    BOOST_CHECK_EQUAL(request.reqParamUInt64("u64"), u64);
    BOOST_CHECK_EQUAL(request.optParamUInt64("missing", 99ULL), 99ULL);

    BOOST_CHECK_CLOSE(request.reqParamDouble("dbl"), 1.25, 0.0001);
    BOOST_CHECK_CLOSE(request.optParamDouble("missing", 2.5), 2.5, 0.0001);

    vector<string> const strVec = request.reqParamStringVec("strVec");
    BOOST_REQUIRE_EQUAL(strVec.size(), 2U);
    BOOST_CHECK_EQUAL(strVec[0], "one");
    BOOST_CHECK_EQUAL(strVec[1], "two");
    vector<string> const defaultStrVec = {"a", "b"};
    BOOST_CHECK(request.optParamStringVec("missing", defaultStrVec) == defaultStrVec);

    vector<uint64_t> const u64Vec = request.reqParamUInt64Vec("u64Vec");
    BOOST_REQUIRE_EQUAL(u64Vec.size(), 2U);
    BOOST_CHECK_EQUAL(u64Vec[0], 5ULL);
    BOOST_CHECK_EQUAL(u64Vec[1], 7ULL);
    vector<uint64_t> const defaultU64Vec = {8ULL, 9ULL};
    BOOST_CHECK(request.optParamUInt64Vec("missing", defaultU64Vec) == defaultU64Vec);

    json const& arr = request.reqParamVec("arr");
    BOOST_CHECK(arr.is_array());
    BOOST_CHECK_EQUAL(arr.size(), 3U);

    json const& obj = request.reqParamObj("obj");
    BOOST_CHECK(obj.is_object());
    BOOST_CHECK_EQUAL(obj.at("k").get<string>(), "v");
}

BOOST_AUTO_TEST_CASE(WorkerHttpRequestReqParamUInt32EdgeCases) {
    uint32_t const u32Max = std::numeric_limits<uint32_t>::max();

    TestWorkerHttpRequest const request({
            {"u32Zero", 0U},
            {"u32Max", static_cast<uint64_t>(u32Max)},
            {"u32TooBig", static_cast<uint64_t>(u32Max) + 1ULL},
            {"u32SignedNegative", -1},
            {"u32Float", 1.5},
            {"u32String", "123"},
    });

    BOOST_CHECK_EQUAL(request.reqParamUInt32("u32Zero"), 0U);
    BOOST_CHECK_EQUAL(request.reqParamUInt32("u32Max"), u32Max);
    BOOST_CHECK_EQUAL(request.optParamUInt32("missingU32", 17U), 17U);

    BOOST_CHECK_THROW(request.reqParamUInt32("u32TooBig"), std::exception);
    BOOST_CHECK_THROW(request.reqParamUInt32("u32SignedNegative"), std::exception);
    BOOST_CHECK_THROW(request.reqParamUInt32("u32Float"), std::exception);
    BOOST_CHECK_THROW(request.reqParamUInt32("u32String"), std::exception);
}

BOOST_AUTO_TEST_CASE(WorkerHttpRequestParsingExceptions) {
    TestWorkerHttpRequest const request({
            {"string", "abc"},
            {"u16", 65535U},
            {"arr", json::array({1, 2, 3})},
            {"obj", json::object({{"k", "v"}})},
            {"tooBigU16", 65536U},
            {"tooBigI32", 2147483648LL},
            {"badStrVec", json::array({"ok", 9})},
            {"badU64Vec", json::array({1, -1})},
    });

    BOOST_CHECK_THROW(request.reqParamString("missing"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamString("u16"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamBool("string"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamUInt16("tooBigU16"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamInt32("tooBigI32"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamDouble("u16"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamStringVec("badStrVec"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamUInt64Vec("badU64Vec"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamVec("obj"), invalid_argument);
    BOOST_CHECK_THROW(request.reqParamObj("arr"), invalid_argument);
}

BOOST_AUTO_TEST_SUITE_END()
