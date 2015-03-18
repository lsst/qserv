// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  /**
  *
  * @brief Test for DuplicateSelectExprPlugin
  *
  */

// System headers
#include <sstream>

// Third-party headers
#include "boost/format.hpp"
#include "boost/make_shared.hpp"

// Qserv headers+
#include "DuplSelectExprPlugin.h"
#include "util/Error.h"
#include "util/MultiError.h"

// Boost unit test header
#define BOOST_TEST_MODULE SelectList
#include "boost/test/included/unit_test.hpp"

namespace lsst {
namespace qserv {
namespace qana {

namespace test
{

    /**
     * Used to test DuplSelectExprPlugin private methods
     */
    class DuplSelectExprPluginTestHelper
    {
        public:
        DuplSelectExprPluginTestHelper()
        {
            plugin = DuplSelectExprPlugin();
        }

        /**
         * Used to test getDuplicateAndPosition()
         *
         * getDuplicateAndPosition() algorithm presents
         * a strong complexity w.r.t its short size
         *
         * @see DuplSelectExprPlugin::getDuplicateAndPosition(StringVector const& v)
         */
        util::MultiError getDuplicateAndPosition(StringVector const& v)
        {
            util::MultiError errors = plugin.getDuplicateAndPosition(v);
            return errors;
        }

        private:
            DuplSelectExprPlugin plugin;
    };
}

struct TestFixture {
    test::DuplSelectExprPluginTestHelper testPlugin;
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(getDuplicateAndPosition) {

    std::vector<std::string> v;
    v.push_back("sum(pm_declerr)");
    v.push_back("f1");
    v.push_back("f1");
    v.push_back("avg(pm_declerr)");

    util::MultiError errors = testPlugin.getDuplicateAndPosition(v);

    std::stringstream sstm;
    sstm << "\t[" << util::Error::DUPLICATE_SELECT_EXPR << "] " << DuplSelectExprPlugin::ERR_MSG;
    std::string err_msg_template = sstm.str();

    boost::format dupl_field_err_msg = boost::format(err_msg_template) %
                            "f1" % " 2 3";
    std::string expected_err_msg = util::MultiError::HEADER_MSG +dupl_field_err_msg.str();

    BOOST_CHECK_EQUAL(errors.toString(), expected_err_msg);
}

BOOST_AUTO_TEST_SUITE_END()

}}} // lsst::qserv::query
