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
#ifndef LSST_QSERV_GLOBAL_UBERJOBBASE_H
#define LSST_QSERV_GLOBAL_UBERJOBBASE_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "global/intTypes.h"

namespace lsst::qserv {

/// Base class for UberJobs. Expected children are
///   qdisp::UberJob - track and expedite an UberJob for qdisp::Executive on a czar.
///   wbase::UberJobData - track Task objects being run for an UberJob on the worker.
class UberJobBase : public std::enable_shared_from_this<UberJobBase> {
public:
    using Ptr = std::shared_ptr<UberJobBase>;

    UberJobBase() = delete;
    UberJobBase(UberJobBase const&) = delete;
    UberJobBase& operator=(UberJobBase const&) = delete;

    virtual ~UberJobBase() = default;

    virtual std::string cName(const char* funcN) const {
        return std::string("UberJobBase::") + funcN + " " + getIdStr();
    }

    QueryId getQueryId() const { return _queryId; }
    UberJobId getUjId() const { return _uberJobId; }
    CzarId getCzarId() const { return _czarId; }
    std::string const& getIdStr() const { return _idStr; }

    virtual std::ostream& dumpOS(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& os, UberJobBase const& uj);

protected:
    UberJobBase(QueryId queryId_, UberJobId uberJobId_, CzarId czarId_)
            : _queryId(queryId_),
              _uberJobId(uberJobId_),
              _czarId(czarId_),
              _idStr("QID=" + std::to_string(queryId_) + "_ujId=" + std::to_string(uberJobId_)) {}

    QueryId const _queryId;
    UberJobId const _uberJobId;
    CzarId const _czarId;  ///< At some point in the future, changing czarId may be possible.
    std::string const _idStr;
};

}  // namespace lsst::qserv

#endif  // LSST_QSERV_GLOBAL_UBERJOBBASE_H
