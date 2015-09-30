/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_CSS_NODEPARAMS_H
#define LSST_QSERV_CSS_NODEPARAMS_H

// System headers
#include <string>

// Third-party headers

// Qserv headers


namespace lsst {
namespace qserv {
namespace css {

/// @addtogroup css

/**
 *  @ingroup css
 *
 *  @brief Metadata describing a Node in CSS.
 */

struct NodeParams {
    NodeParams() : port(0) {}
    NodeParams(std::string const& type_, std::string const& host_,
               int port_, std::string const& status_) :
                   type(type_), host(host_), port(port_), status(status_) {}

    std::string type;    ///< Node type, e.g. "worker" or "czar"
    std::string host;    ///< Host name or IP address
    int port;            ///< Port number of wmgr service
    std::string status;  ///< Node status, e.g. "ACTIVE" or "INACTIVE"

    // returns true if node is active
    bool isActive() const { return status == "ACTIVE"; }
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_NODEPARAMS_H
