// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
 *
 *  @author: John Gates,
 */

#ifndef LSST_QSERV_UTIL_COMMAND_H_
#define LSST_QSERV_UTIL_COMMAND_H_

// System headers
#include <condition_variable>
#include <memory>
#include <mutex>


namespace lsst {
namespace qserv {
namespace util {

/// Tracker provides an interface for indicating an action is complete.
///
class Tracker {
public:
    Tracker() {};
    virtual ~Tracker() {};
    enum class Status { INPROGRESS, COMPLETE };
    using Ptr = std::shared_ptr<Tracker>;
    void setComplete();
    bool isFinished();
    void waitComplete();
private:
    Status _trStatus{Status::INPROGRESS};
    std::mutex _trMutex;
    std::condition_variable _trCV;
};

/// Base class for commands. Can be used with functions as is or
/// as a base class when data is needed.
class Command {
public:
    using Ptr = std::shared_ptr<Command>;
    Command() {};
    Command(std::function<void()> func) : _func{func} {}
    virtual ~Command() {};
    virtual int action() {
        _func();
        return 0;
    };
    void setFunc(std::function<void()> func);
    void resetFunc();
protected:
    std::function<void()> _func = [](){;};
};

/// Extension of Command that can notify other threads when its
/// action is complete.
class CommandTracked : public virtual Command, public virtual Tracker {
public:
    using Ptr = std::shared_ptr<CommandTracked>;
    CommandTracked() {};
    CommandTracked(std::function<void()> func) : Command{func} {}
    virtual ~CommandTracked() {};
    int action() override {
        _func();
        setComplete();
        return 0;
    };
};

}}} // namespace

#endif /* CORE_MODULES_UTIL_COMMAND_H_ */
