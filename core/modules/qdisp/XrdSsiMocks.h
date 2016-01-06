// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
 *      @author: John Gates, SLAC
 */

#ifndef LSST_QSERV_QDISP_XRDSSIMOCKS_H
#define LSST_QSERV_QDISP_XRDSSIMOCKS_H


// External headers
#include "XrdSsi/XrdSsiService.hh"
#include "XrdSsi/XrdSsiSession.hh"
#include "XrdSsi/XrdSsiRequest.hh"

// Local headers
#include "qdisp/QueryRequest.h"
#include "qdisp/QueryResource.h"
#include "util/threadSafe.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class Executive;

/** A greatly simplified version of XrdSsiService for testing the Executive class.
 */
class XrdSsiServiceMock : public XrdSsiService
{
public:
    virtual void Provision(Resource *resP, unsigned short timeOut=0, bool userConn=false);
    XrdSsiServiceMock(Executive *executive) {};
    void setGo(bool go) {
        _go.set(go);
    }
protected:
    void mockProvisionTest(QueryResource *resP, unsigned short timeOut);
public:
    virtual ~XrdSsiServiceMock() {}
    static util::FlagNotify<bool> _go;
    static util::Sequential<int> _count;
};

/** Class used to fake calls to XrdSsiSession::ProcessRequest.
 */
class XrdSsiSessionMock : public XrdSsiSession
{
public:
    XrdSsiSessionMock(char *sname, char *sloc=0) : XrdSsiSession(sname, sloc) {}
    virtual ~XrdSsiSessionMock() {}
    static const char* getMockString() {
        return "MockTrue";
    }
    virtual void ProcessRequest(XrdSsiRequest *reqP, unsigned short tOut);

    virtual bool Unprovision(bool forced=false){return true;};

protected:
    virtual void RequestFinished(XrdSsiRequest  *rqstP, const XrdSsiRespInfo &rInfo, bool cancel=false) {};
};

}}} // namespace

#endif
