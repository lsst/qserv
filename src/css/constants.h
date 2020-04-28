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
// Another place where version number appears is tests/testKvMap.h.
char const VERSION_KEY[] = "/css_meta/version"; ///< Path to version
int const VERSION = 1; ///< Current supported version (integer)
// kvInterface treats everything as strings, so to avoid multiple
// conversions I define this string once and use it with kvInterface
char const VERSION_STR[] = "1"; ///< Current supported version

// Set of values used for database and table status.

/// This status means CSS data is in inconsistent state, do not use.
/// Typically used only when constructing new objects.
char const KEY_STATUS_IGNORE[] = "DO_NOT_USE";

/// Item is ready, meaning it exists both in CSS and on all workers
char const KEY_STATUS_READY[] = "READY";

/// Item is created in CSS, needs to be created on workers,
/// this is the prefix followed by timestamp.
char const KEY_STATUS_CREATE_PFX[] = "PENDING_CREATE:";

/// Item is to be removed from workers and CSS,
/// this is the prefix followed by timestamp.
char const KEY_STATUS_DROP_PFX[] = "PENDING_DROP:";

/// Some requested operation failed (e.g. after PENDING_CREATE
/// watcher failed to create objects on workers),
/// this is the prefix followed by arbitrary message.
char const KEY_STATUS_FAILED_PFX[] = "FAILED:";

/// Node state, "ACTIVE" means can be used for regular work
char const NODE_STATE_ACTIVE[] = "ACTIVE";

/// Node state, "ACTIVE" means can be used for regular work
char const NODE_STATE_INACTIVE[] = "INACTIVE";

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_CONSTANTS_H
