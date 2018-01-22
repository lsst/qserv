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
 */
#ifndef LSST_QSERV_XRDSVC_SSISESSION_H
#define LSST_QSERV_XRDSVC_SSISESSION_H

// System headers
#include <atomic>
#include <mutex>
#include <vector>

// Third-party headers
#include "XrdSsi/XrdSsiResponder.hh"

// Local headers
#include "global/ResourceUnit.h"
#include "wbase/Task.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace xrdsvc {

/// An implementation of XrdSsiResponder that is used by SsiService to provide
/// qserv worker services. The SSI interface encourages such an approach, and
/// object lifetimes are explicitly stated in the documentation which we
/// adhere to using BindRequest() and UnBindRequest() responder methods.
class SsiSession : public XrdSsiResponder, public std::enable_shared_from_this<SsiSession> {
public:
    typedef std::shared_ptr<ResourceUnit::Checker> ValidatorPtr;
    typedef std::shared_ptr<SsiSession> Ptr;

    // Use factory to ensure proper construction for enable_shared_from_this.
    static SsiSession::Ptr newSsiSession(std::string const& rname,
               ValidatorPtr validator,
               std::shared_ptr<wbase::MsgProcessor> const& processor) {
        SsiSession::Ptr ssiPtr(new SsiSession(rname, validator, processor));
        return ssiPtr;
    }

    virtual ~SsiSession();

    void execute(XrdSsiRequest& req);

    // XrdSsiResponder interfaces
    void Finished(XrdSsiRequest& req, XrdSsiRespInfo const& rinfo,
                  bool cancel=false) override;

private:
    /// Construct a new session (called by SsiService)
    SsiSession(std::string const& rname,
               ValidatorPtr validator,
               std::shared_ptr<wbase::MsgProcessor> const& processor)
        : _validator(validator), _processor(processor), _resourceName(rname) {}
    class ReplyChannel;
    friend class ReplyChannel;

    ValidatorPtr _validator; ///< validates request against what's available
    std::shared_ptr<wbase::MsgProcessor> _processor; ///< actual msg processor

    std::mutex  _finMutex;  ///< Protects execute() from Finish()
    std::string _resourceName;
};
}}} // namespace

#endif // LSST_QSERV_XRDSVC_SSISESSION_H
