// -*- LSST-C++ -*-

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
#ifndef LSST_QSERV_WBASE_USERQUERYINFO_H
#define LSST_QSERV_WBASE_USERQUERYINFO_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <vector>

// Qserv headers
#include "global/intTypes.h"

// This header declarations
namespace lsst::qserv::wbase {

/// This class contains information about a user query that is effectively the same
/// for all Task's in the user query.
class UserQueryInfo {
public:
    using Ptr = std::shared_ptr<UserQueryInfo>;
    using Map = std::map<QueryId, std::weak_ptr<UserQueryInfo>>;

    static Ptr uqMapInsert(QueryId qId);
    static Ptr uqMapGet(QueryId qId);
    /// Erase the entry for `qId` in the map, as long as there are only
    /// weak references to the UserQueryInfoObject.
    /// Clear appropriate local and member references before calling this.
    static void uqMapErase(QueryId qId);

    UserQueryInfo(QueryId qId);
    UserQueryInfo() = delete;
    UserQueryInfo(UserQueryInfo const&) = delete;
    UserQueryInfo& operator=(UserQueryInfo const&) = delete;

    ~UserQueryInfo() = default;

    /// Add a query template to the map of templates for this user query.
    size_t addTemplate(std::string const& templateStr);

    /// Retrieve the template associated with the key 'id' from the map of templates.
    /// @throws Bug if id is out of range.
    std::string getTemplate(size_t id);

private:
    static Map _uqMap;
    static std::mutex _uqMapMtx;  ///< protects _uqMap

    QueryId const _qId;  ///< The User Query Id number.

    /// List of template strings. This is expected to be short, 1 or 2 entries.
    std::vector<std::string> _templates;
    std::mutex _uqMtx;  ///< protects _templates;
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_USERQUERYINFO_H
