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
#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE ObjectIndex
#include "boost/test/unit_test.hpp"

#include "partition/ConfigStore.h"
#include "TempFile.h"

#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <string>
#include <vector>

#include "boost/any.hpp"
#include "boost/program_options.hpp"
#include "boost/filesystem.hpp"
#include "nlohmann/json.hpp"

using json = nlohmann::json;

using namespace lsst::partition;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

BOOST_AUTO_TEST_CASE(ConfigStoreTest) {
    // Constructors

    BOOST_REQUIRE_NO_THROW({ ConfigStore const store; });

    json const emptyConfig;
    BOOST_REQUIRE_NO_THROW({ ConfigStore const store(emptyConfig); });

    json const emptyConfigObject = json::object();
    BOOST_REQUIRE_NO_THROW({ ConfigStore const store(emptyConfigObject); });

    auto const wrongConfig = R"(
        ["p1","p2"]
    )"_json;
    BOOST_CHECK_THROW({ ConfigStore const store(wrongConfig); }, std::invalid_argument);

    auto const simpleConfig = R"(
        {
            "n":1.1,
            "s":"abc",
            "c":"\t",
            "v":["t","u","v"],
            "d":{
                "p1":123,
                "p2":"xyz",
                "v1":[1,2,3,4],
                "v2":[[1,2],[3,4]],
                "dd":{
                    "pp":"11"
                }
            },
            "flag_true": true,
            "flag_false": false
        }
    )"_json;
    std::unique_ptr<ConfigStore> simpleStorePtr;
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr.reset(new ConfigStore(simpleConfig)); });

    // Accessors

    BOOST_CHECK_THROW(
            {
                // Empty path to a parameter isn't allowed
                simpleStorePtr->has("");
            },
            std::invalid_argument);

    BOOST_REQUIRE_NO_THROW({
        // Non-existing parameter
        simpleStorePtr->has("a");
    });
    BOOST_REQUIRE_NO_THROW({
        // Existing parameter
        simpleStorePtr->has("n");
    });
    BOOST_CHECK_EQUAL(simpleStorePtr->has("a"), false);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("n"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("s"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("c"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("v"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.p1"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.p2"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.v1"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.v2"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.dd"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.dd.pp"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.dd.ppp"), false);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.ddd"), false);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("flag_true"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("flag_false"), true);

    // Locators for non-valid parameters

    BOOST_CHECK_THROW(
            {
                // Empty path to a parameter isn't allowed
                simpleStorePtr->get<std::string>("");
            },
            std::invalid_argument);

    BOOST_CHECK_THROW(
            {
                // Non-existing parameter
                simpleStorePtr->get<std::string>("a");
            },
            std::invalid_argument);

    // Locators and type conversion for valid parameters

    BOOST_CHECK_THROW(
            {
                // Using an incorrect type to extract a value
                simpleStorePtr->get<std::string>("n");
            },
            ConfigTypeError);

    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<double>("n"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::string>("s"); });

    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::string>("c"); });
    BOOST_REQUIRE_NO_THROW({
        // A single-character string is allowed to be interpreted as a value of the 'char' type.
        // This would be required for parameters read from the JSON files since JSON doesn't
        // explicitly support the single character type.
        simpleStorePtr->get<char>("c");
    });

    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::vector<std::string>>("v"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<int>("d.p1"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::string>("d.p2"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::vector<int>>("d.v1"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::vector<std::vector<int>>>("d.v2"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::string>("d.dd.pp"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<bool>("flag_true"); });
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<bool>("flag_false"); });

    BOOST_CHECK_EQUAL(simpleStorePtr->get<double>("n"), 1.1);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("s"), std::string("abc"));
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("c"), std::string("\t"));

    // Another way of solving a problem of interpreting a single-character string
    // as a value of the 'char' type.
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("c").size(), 1U);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("c")[0], '\t');

    std::vector<std::string> const pv = simpleStorePtr->get<std::vector<std::string>>("v");
    std::vector<std::string> const v = {"t", "u", "v"};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(pv.size(), v.size());
        for (size_t i = 0, n = pv.size(); i < n; ++i) {
            BOOST_CHECK_EQUAL(pv[i], v[i]);
        }
    });

    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d.p1"), 123);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("d.p2"), std::string("xyz"));

    std::vector<int> const dpv1 = simpleStorePtr->get<std::vector<int>>("d.v1");
    std::vector<int> const dv1 = {1, 2, 3, 4};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(dpv1.size(), dv1.size());
        for (size_t i = 0, n = dpv1.size(); i < n; ++i) {
            BOOST_CHECK_EQUAL(dpv1[i], dv1[i]);
        }
    });
    std::vector<std::vector<int>> const dpv2 = simpleStorePtr->get<std::vector<std::vector<int>>>("d.v2");
    std::vector<std::vector<int>> const dv2 = {{1, 2}, {3, 4}};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(dpv2.size(), dv2.size());
        for (size_t i = 0, n = dpv2.size(); i < n; ++i) {
            auto const& pv = dpv2[i];
            auto const& v = dv2[i];
            BOOST_CHECK_EQUAL(pv.size(), v.size());
            for (size_t j = 0, n = pv.size(); j < n; ++j) {
                BOOST_CHECK_EQUAL(pv[j], v[j]);
            }
        }
    });
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("d.dd.pp"), std::string("11"));
    BOOST_CHECK_EQUAL(simpleStorePtr->get<bool>("flag_true"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->flag("flag_true"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<bool>("flag_false"), false);
    BOOST_CHECK_EQUAL(simpleStorePtr->flag("flag_false"), false);

    // Parameters which are not of type 'bool' should not be used as 'flags'
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->get<std::string>("s"); });
    BOOST_CHECK_THROW({ simpleStorePtr->flag("s"); }, std::exception);

    // Adding new or modifying existing parameters

    BOOST_CHECK_THROW(
            {
                // Empty path is not allowed
                simpleStorePtr->set<int>("", 1);
            },
            std::invalid_argument);

    BOOST_CHECK_EQUAL(simpleStorePtr->has("a"), false);
    BOOST_REQUIRE_NO_THROW({
        // This should create a new parameters
        simpleStorePtr->set<int>("a", 2);
    });
    BOOST_CHECK_EQUAL(simpleStorePtr->has("a"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("a"), 2);

    BOOST_CHECK_EQUAL(simpleStorePtr->get<double>("n"), 1.1);
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->set<double>("n", 2.2); });
    BOOST_CHECK_EQUAL(simpleStorePtr->get<double>("n"), 2.2);

    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d.p1"), 123);
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->set<int>("d.p1", 456); });
    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d.p1"), 456);

    BOOST_CHECK_EQUAL(simpleStorePtr->has("d1.a"), false);
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->set<int>("d1.a", 987); });
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d1.a"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d1.a"), 987);

    // Test adding more parameters from a JSON object

    json const simpleArray = json::array({1, 2, 3, 4});
    BOOST_CHECK_THROW(
            {
                // JSON arrays are not allowed
                simpleStorePtr->add(simpleArray);
            },
            std::invalid_argument);

    auto const extendedConfig = R"(
        {
            "k":"kvc",
            "s":"def",
            "d":{
                "p1":789,
                "p3":"xyz"
            }
        }
    )"_json;

    BOOST_CHECK_EQUAL(simpleStorePtr->has("k"), false);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("s"), std::string("abc"));
    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d.p1"), 456);
    BOOST_CHECK_EQUAL(simpleStorePtr->has("d.p3"), false);
    BOOST_REQUIRE_NO_THROW({ simpleStorePtr->add(extendedConfig); });
    BOOST_CHECK_EQUAL(simpleStorePtr->has("k"), true);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("s"), std::string("def"));
    BOOST_CHECK_EQUAL(simpleStorePtr->get<int>("d.p1"), 789);
    BOOST_CHECK_EQUAL(simpleStorePtr->get<std::string>("d.p3"), std::string("xyz"));

    // Test loading parameters from a JSON file

    TempFile t;
    std::string const filename = t.path().string();

    auto const fileConfig = R"(
        {
            "a":"abc",
            "b":1,
            "c":{
                "d":2,
                "e":"efg",
                "f":["one","two","three"]
            }
        }
    )"_json;
    std::ofstream outfile(filename);
    outfile << fileConfig;
    outfile.close();

    ConfigStore anotherConfig;

    BOOST_CHECK_THROW(
            {
                // Empty filenames aren't allowed
                anotherConfig.parse("");
            },
            std::invalid_argument);

    BOOST_CHECK_EQUAL(anotherConfig.has("a"), false);
    BOOST_CHECK_EQUAL(anotherConfig.has("b"), false);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.d"), false);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.e"), false);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.f"), false);
    BOOST_REQUIRE_NO_THROW({ anotherConfig.parse(filename); });
    BOOST_CHECK_EQUAL(anotherConfig.has("a"), true);
    BOOST_CHECK_EQUAL(anotherConfig.has("b"), true);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.d"), true);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.e"), true);
    BOOST_CHECK_EQUAL(anotherConfig.has("c.f"), true);

    BOOST_CHECK_EQUAL(anotherConfig.get<std::string>("a"), std::string("abc"));
    BOOST_CHECK_EQUAL(anotherConfig.get<int>("b"), 1);
    BOOST_CHECK_EQUAL(anotherConfig.get<int>("c.d"), 2);
    BOOST_CHECK_EQUAL(anotherConfig.get<std::string>("c.e"), std::string("efg"));

    std::vector<std::string> const pcf = anotherConfig.get<std::vector<std::string>>("c.f");
    std::vector<std::string> const cf = {"one", "two", "three"};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(pcf.size(), cf.size());
        for (size_t i = 0, n = pcf.size(); i < n; ++i) {
            BOOST_CHECK_EQUAL(pcf[i], cf[i]);
        }
    });

    // Test loading parameters from Boost command options

    po::variables_map vm;
    bool const defaulted = true;
    std::string a = "abcd";
    std::vector<std::string> b = {"one", "two", "three", "four"};
    char c = '\t';
    int d = 1;
    uint32_t e = 2U;
    size_t f = 3U;
    float g = 4.4;
    double h = 5.5;
    int dda = 6;
    bool f_true = true;
    bool f_false = true;
    vm.insert(make_pair(std::string("a"), po::variable_value(boost::any(a), defaulted)));
    vm.insert(make_pair(std::string("b"), po::variable_value(boost::any(b), defaulted)));
    vm.insert(make_pair(std::string("c"), po::variable_value(boost::any(c), defaulted)));
    vm.insert(make_pair(std::string("d"), po::variable_value(boost::any(d), defaulted)));
    vm.insert(make_pair(std::string("e"), po::variable_value(boost::any(e), defaulted)));
    vm.insert(make_pair(std::string("f"), po::variable_value(boost::any(f), defaulted)));
    vm.insert(make_pair(std::string("g"), po::variable_value(boost::any(g), defaulted)));
    vm.insert(make_pair(std::string("h"), po::variable_value(boost::any(h), defaulted)));
    vm.insert(make_pair(std::string("dd.a"), po::variable_value(boost::any(dda), defaulted)));
    vm.insert(make_pair(std::string("f_true"), po::variable_value(boost::any(f_true), defaulted)));
    vm.insert(make_pair(std::string("f_false"), po::variable_value(boost::any(f_false), defaulted)));

    ConfigStore nextConfig;

    BOOST_CHECK_EQUAL(nextConfig.has("a"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("b"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("c"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("d"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("e"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("f"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("g"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("h"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("dd.a"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("f_true"), false);
    BOOST_CHECK_EQUAL(nextConfig.has("f_false"), false);
    BOOST_REQUIRE_NO_THROW({ nextConfig.add(vm); });
    BOOST_CHECK_EQUAL(nextConfig.has("a"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("b"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("c"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("d"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("e"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("f"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("g"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("h"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("dd.a"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("f_true"), true);
    BOOST_CHECK_EQUAL(nextConfig.has("f_false"), true);

    BOOST_CHECK_EQUAL(nextConfig.get<decltype(a)>("a"), a);

    decltype(b) nb = nextConfig.get<decltype(b)>("b");
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(nb.size(), b.size());
        for (size_t i = 0, n = nb.size(); i < n; ++i) {
            BOOST_CHECK_EQUAL(nb[i], b[i]);
        }
    });
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(c)>("c"), c);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(d)>("d"), d);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(e)>("e"), e);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(f)>("f"), f);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(g)>("g"), g);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(h)>("h"), h);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(dda)>("dd.a"), dda);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(f_true)>("f_true"), f_true);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(f_false)>("f_false"), f_false);

    // Test for unsupported types

    struct MyType {};
    MyType mt;
    vm.clear();
    vm.insert(make_pair(std::string("mt"), po::variable_value(boost::any(mt), defaulted)));

    BOOST_CHECK_THROW({ nextConfig.add(vm); }, ConfigTypeError);

    // Test for cases when values of existing parameters in the store should not
    // be updated from the command line option for default values of the parameters
    // if the parameter was already present in the store.

    std::string a2 = a + a;

    vm.clear();
    vm.insert(make_pair(std::string("a"), po::variable_value(boost::any(a2), defaulted)));

    BOOST_CHECK_EQUAL(nextConfig.has("a"), true);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(a)>("a"), a);
    BOOST_REQUIRE_NO_THROW({
        // This should NOT update the stored parameter's value since parameter already exist
        // in the store and the new value is 'defaulted'.
        nextConfig.add(vm);
    });
    BOOST_CHECK_EQUAL(nextConfig.has("a"), true);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(a)>("a"), a);

    vm.clear();
    vm.insert(make_pair(std::string("a"), po::variable_value(boost::any(a2), !defaulted)));

    BOOST_CHECK_EQUAL(nextConfig.has("a"), true);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(a)>("a"), a);
    BOOST_REQUIRE_NO_THROW({
        // This SHOULD update the stored parameter's value since the new value is NOT 'defaulted'.
        nextConfig.add(vm);
    });
    BOOST_CHECK_EQUAL(nextConfig.has("a"), true);
    BOOST_CHECK_EQUAL(nextConfig.get<decltype(a)>("a"), a2);

    // Test a special case of a command-line parameter which has no value, neither it has
    // any default value. In this case a client would interpret a presence of the parameter
    // in a command line as a flag.

    boost::any emptyValue;

    vm.clear();
    vm.insert(make_pair(std::string("flag"), po::variable_value(emptyValue, defaulted)));

    BOOST_CHECK_EQUAL(nextConfig.has("flag"), false);
    BOOST_CHECK_EQUAL(nextConfig.flag("flag"), false);
    BOOST_REQUIRE_NO_THROW({
        // This should NOT update the stored parameter's value
        nextConfig.add(vm);
    });
    BOOST_CHECK_EQUAL(nextConfig.has("flag"), true);
    BOOST_CHECK_EQUAL(nextConfig.get<bool>("flag"), true);
    BOOST_CHECK_EQUAL(nextConfig.flag("flag"), true);
}
