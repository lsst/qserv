// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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

#ifndef LSST_QSERV_CSS_CSSCONFIG_H
#define LSST_QSERV_CSS_CSSCONFIG_H

// System headers
#include <map>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

namespace lsst {
namespace qserv {
namespace css {

/**
 *  Provide all configuration parameters for a Qserv CSS instance
 *
 *  Use a collection of (key, value) as input, identify required parameters and ignore
 *  others, analyze and store them inside private member variables, use default
 *  values for missing parameters, provide accessor for each of these variable.
 *  This class hide configuration complexity from other part of the code.
 *  All private member variables are related to CSS parameters and are immutables.
 *
 */
class CssConfig {
public:

    /**
     *  Create CssConfig instance from a collection of (key, value)
     *
     *  @param configMap: a collection of (key, value)
     */
    CssConfig(std::map<std::string, std::string> configMap)
        : CssConfig(util::ConfigStore(configMap)) {
    }

    CssConfig(CssConfig const&) = delete;
    CssConfig& operator=(CssConfig const&) = delete;

    /** Overload output operator for current class
     *
     * @param out
     * @param workerConfig
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, CssConfig const& cssConfig);

    mysql::MySqlConfig const& getMySqlConfig() const {
        return _mySqlConfig;
    }

    /* Get key-value data used to initialize CSS
     *
     * @see testData variable in core/modules/css/testCssAccess.c for example
     *
     * @return key-value data used to initialize CSS
     */
    std::string const& getData() const {
        return _data;
    }

    /* Get key-value data used to initialize CSS
         *
         * @see testData variable in core/modules/css/testCssAccess.c for example
         *
         * @return key-value data used to initialize CSS
         */
    std::string const& getFile() const {
        return _file;
    }

    /* Get thread pool size for shared scans
     *
     * @return thread pool size for shared scans
     */
    std::string const& getTechnology() const {
        return _technology;
    }

private:

    CssConfig(util::ConfigStore const& configStore);

    std::string const _technology;

    // used by "mem" technology
    std::string const _data;
    std::string const _file;

    // used by "mysql" technology
    mysql::MySqlConfig const _mySqlConfig;

};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_CSSCONFIG_H
