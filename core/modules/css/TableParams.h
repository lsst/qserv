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
#ifndef LSST_QSERV_CSS_TABLEPARAMS_H
#define LSST_QSERV_CSS_TABLEPARAMS_H

// System headers

// Third-party headers

// Qserv headers
#include "css/MatchTableParams.h"
#include "css/PartTableParams.h"

namespace lsst {
namespace qserv {
namespace css {

/// @addtogroup css

/**
 *  @ingroup css
 *
 *  @brief Class defining a set of table partitioning parameters.
 *
 *  This class is just a combination of the PartTableParams and
 *  MatchTableParams.
 */

struct TableParams {

    MatchTableParams match;          ///< match metadata
    PartTableParams partitioning;    ///< partitioning metadata

};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_TABLEPARAMS_H
