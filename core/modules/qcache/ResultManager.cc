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
#include "qcache/ResultManager.h"

// Qserv headers
#include "qcache/Exceptions.h"
#include "qcache/MySQLConnection.h"
#include "qcache/MySQLconnPool.h"
#include "qcache/Page.h"
#include "qcache/PageData.h"
#include "qcache/PageHolder.h"
#include "qcache/Pool.h"

// System headers
#include <stdexcept>

namespace lsst {
namespace qserv {
namespace qcache {

std::shared_ptr<ResultManager> ResultManager::create(std::string const& queryId,
                                                     std::string const& taskId,
                                                     std::shared_ptr<MySQLconnPool> const& connPool,
                                                     std::shared_ptr<Pool> const& pagePool,
                                                     boost::asio::io_service& io_service,
                                                     std::size_t expirationTimeoutSec) {
    return std::shared_ptr<ResultManager>(new ResultManager(
        queryId, taskId, connPool, pagePool, io_service, expirationTimeoutSec
    ));
}


ResultManager::ResultManager(std::string const& queryId,
                             std::string const& taskId,
                             std::shared_ptr<MySQLconnPool> const& connPool,
                             std::shared_ptr<Pool> const& pagePool,
                             boost::asio::io_service& io_service,
                             std::size_t expirationTimeoutSec)
    :   _queryId(queryId),
        _taskId(taskId),
        _connPool(connPool),
        _pagePool(pagePool),
        _io_service(io_service),
        _expirationTimeoutSec(expirationTimeoutSec)  {
}


void ResultManager::execute(std::string const& query) {
    std::string const context = "ResultManager::" + std::string(__func__) + " ";
    std::lock_guard lock(_mtx);
    if (_state != State::INITIAL) {
        throw std::logic_error(
                context + "incorrect use of the manager. A transaction may have been executed.");
    }
    _conn = _connPool->allocate();
    _state = State::EXECUTING;
    try {
        _conn->execute(query);
    } catch (std::exception const& ex) {
        _state = State::EXECUTE_FAILED;
        _connPool->release(_conn);
        _conn = nullptr;
        throw std::logic_error(
                context + "query has failed, ex: " + std::string(ex.what) + ", query: " + query);
    }
    _state = State::EXECUTED;
}


bool ResultManager::isGood() const {
    std::lock_guard lock(_mtx);
    return true;
}


bool ResultManager::isReady() const {
    std::lock_guard lock(_mtx);
    return true;
}


bool ResultManager::isComplete() const {
    std::lock_guard lock(_mtx);
    return true;
}


std::size_t ResultManager::sizeBytes() const {
    std::lock_guard lock(_mtx);
    return 0;
}


std::size_t ResultManager::sizeRows() const {
    std::lock_guard lock(_mtx);
    return 0;
}


std::size_t ResultManager::sizePages() const {
    std::lock_guard lock(_mtx);
    return 0;
}


std::shared_ptr<PageData> ResultManager::page(std::size_t pageIdx) {
    std::lock_guard lock(_mtx);
    _assertPageIsValid(lock, __func__, pageIdx);
    return _pages[pageIdx].acquire(shared_from_this());
}


void ResultManager::clear() {
    std::lock_guard lock(_mtx);
}


void ResultManager::release(std::size_t pageIdx) {
    std::string const context = "ResultManager::" + std::string(__func__) + " ";
    std::lock_guard lock(_mtx);
    _assertPageIsValid(lock, __func__, pageIdx);
    return _pages[pageIdx].release();
}


void ResultManager::_assertPageIsValid(std::lock_guard const& lock,
                                       std::string const& func,
                                       std::size_t pageIdx) const {
    if (pageIdx < _pages.size()) return;
    throw std::out_of_range(
            "ResultManager::" + func +  + " page index " + std::to_string(pageIdx)
            + " is out of range: 0.." + std::to_string(_pages.size()));
}

}}} // namespace lsst::qserv::qcache
