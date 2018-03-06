/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

// Class header
#include "replica/QservMgtServices.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtServices");

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

QservMgtServices::pointer QservMgtServices::create(Configuration::pointer const& configuration) {
    return QservMgtServices::pointer(
        new QservMgtServices(configuration));
}

QservMgtServices::QservMgtServices(Configuration::pointer const& configuration)
    :   _configuration(configuration) {
}

}}} // namespace lsst::qserv::replica