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

// Class header
#include "wbase/UserQueryInfo.h"

// Qserv headers
#include "util/Bug.h"
#include "wbase/Task.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.UserQueryInfo");
}

namespace lsst::qserv::wbase {

UserQueryInfo::UserQueryInfo(QueryId qId) : _qId(qId) {}

UserQueryInfo::Ptr UserQueryInfo::uqMapInsert(QueryId qId) {
    Ptr uqi;
    lock_guard<mutex> lg(_uqMapMtx);
    auto iter = _uqMap.find(qId);
    if (iter != _uqMap.end()) {
        uqi = iter->second.lock();
    }
    // If uqi is invalid at this point, a new one needs to be made.
    if (uqi == nullptr) {
        uqi = make_shared<UserQueryInfo>(qId);
        _uqMap[qId] = uqi;
    }
    return uqi;
}

UserQueryInfo::Ptr UserQueryInfo::uqMapGet(QueryId qId) {
    lock_guard<mutex> lg(_uqMapMtx);
    auto iter = _uqMap.find(qId);
    if (iter != _uqMap.end()) {
        return iter->second.lock();
    }
    return nullptr;
}

void UserQueryInfo::uqMapErase(QueryId qId) {
    lock_guard<mutex> lg(_uqMapMtx);
    auto iter = _uqMap.find(qId);
    if (iter != _uqMap.end()) {
        // If the weak pointer has 0 real references
        if (iter->second.expired()) {
            _uqMap.erase(qId);
        }
    }
}

UserQueryInfo::Map UserQueryInfo::_uqMap;

mutex UserQueryInfo::_uqMapMtx;

size_t UserQueryInfo::addTemplate(std::string const& templateStr) {
    size_t j = 0;
    {
        lock_guard<mutex> lockUq(_uqMtx);
        for (; j < _templates.size(); ++j) {
            if (_templates[j] == templateStr) {
                return j;
            }
        }
        _templates.emplace_back(templateStr);
    }
    LOGS(_log, LOG_LVL_DEBUG, "QueryInfo:" << _qId << " j=" << j << " Added:" << templateStr);
    return j;
}

std::string UserQueryInfo::getTemplate(size_t id) {
    lock_guard<mutex> lockUq(_uqMtx);
    if (id >= _templates.size()) {
        throw util::Bug(ERR_LOC, "UserQueryInfo template index out of range id=" + to_string(id) +
                                         " size=" + to_string(_templates.size()));
    }
    return _templates[id];
}

void UserQueryInfo::appendTasks(vector<shared_ptr<wbase::Task>> const& tasks) {
    lock_guard<mutex> lg(_tasksMtx);
    for (auto const& task : tasks) {
        _tasks.emplace_back(task);
    }
}

/// Cancel all tasks in this user query.
void UserQueryInfo::cancelAllTasks() {
    if (!_userQueryCancelled.exchange(true)) {
        LOGS(_log, LOG_LVL_WARN, "UserQueryInfo::cancelAllTasks " << _qId);
        lock_guard<mutex> lg(_tasksMtx);
        for (auto&& wTask : _tasks) {
            auto task = wTask.lock();
            task->cancel();
        }
    }
}

}  // namespace lsst::qserv::wbase
