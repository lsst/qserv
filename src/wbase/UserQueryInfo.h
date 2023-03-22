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

/// &&& doc
class UserQueryInfo {
public:
    using Ptr = std::shared_ptr<UserQueryInfo>;
    using Map = std::map<QueryId, UserQueryInfo::Ptr>;


    static Ptr uqMapInsert(QueryId qId, std::vector<std::string> const& templateStr);
    static Ptr uqMapGet(QueryId);

    UserQueryInfo(QueryId);
    UserQueryInfo() = delete;
    UserQueryInfo(UserQueryInfo const&) = delete;
    UserQueryInfo& operator=(UserQueryInfo const&) = delete;

    ~UserQueryInfo() = default;

    /// &&& doc
    size_t addTemplate(std::string const& templateStr);

    /// &&& doc
    std::string& getTemplate(size_t id);

private:
    static Map _uqMap;
    static std::mutex _uqMapMtx; ///< protects _uqMap

    QueryId const _qId; ///< The User Query Id number.



    /// List of template strings. This is expected to be short, 1 or 2 entries.
    std::vector<std::string> _templates;

    std::mutex _uqMtx; ///< protects _templates;
};



/* &&&
/// &&& just not working out
/// &&& This is a nuisance. We're trying to save memory, but for quick location the thing we are trying to find
/// needs to be the key, but that's really big. So, we need something that sorts by key string and returns a pointer.
class StringElement : public std::enable_shared_from_this<StringElement> {
public:
    using Ptr = std::shared_ptr<StringElement>;

    StringElement() = delete;

private:
    StringElement(std::string const& str) : _str(str) {    }

    std::string const _str; ///< The string this class is storing, cannot be changed once set.
};



class StringCache {
public:
    class Handle {
    public:
        std::string getStr() { return _str; }
        Handle() = delete;
        Handle(Handle const&) = delete;
        Handle& operator=(Handle const&) = delete;
        ~Handle() {

        }
    private:
        Handle(StringCache& StringCache, std::string const& str) {

        }
        StringCache& _cache;
        std::string const& _str;
    };
private:
    std::map<std::string, int> _strMap;
};
*/



}  // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_USERQUERYDESCRIPTION_H
