// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

#ifndef LSST_QSERV_CSS_CONSTANTS_H
#define LSST_QSERV_CSS_CONSTANTS_H

namespace lsst {
namespace qserv {
namespace css {

// Current version of metadata store.
// VERSION and VERSION_KEY are used by qservAdmin.py and css/Facade .
// Version number is stored in the KV store by qservAdmin when first
// database is created. All other clients are supposed to check stored
// version against compiled-in version and fail if they do not match.
// Another place where version number appears is qproc/testMap.kvmap.
char const VERSION_KEY[] = "/css_meta/version"; ///< Path to version
int const VERSION = 1; ///< Current supported version (integer)
// kvInterface treats everything as strings, so to avoid multiple
// conversions I define this string once and use it with kvInterface
char const VERSION_STR[] = "1"; ///< Current supported version

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_CONSTANTS_H
