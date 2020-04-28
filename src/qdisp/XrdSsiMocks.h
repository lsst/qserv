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
 *      @author: John Gates, SLAC (heavily modified by Andrew Hanushevsky, SLAC)
 */

#ifndef LSST_QSERV_QDISP_XRDSSIMOCKS_H
#define LSST_QSERV_QDISP_XRDSSIMOCKS_H

// External headers
#include "XrdSsi/XrdSsiRequest.hh"
#include "XrdSsi/XrdSsiResource.hh"
#include "XrdSsi/XrdSsiService.hh"

// Local headers

namespace lsst {
namespace qserv {
namespace qdisp {

class Executive;

/** A simplified version of XrdSsiService for testing qserv.
 */
class XrdSsiServiceMock : public XrdSsiService
{
public:
    void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

    XrdSsiServiceMock(Executive *executive) {};

    virtual ~XrdSsiServiceMock() {}

    static int getCount();

    static int getCanCount();

    static int getFinCount();

    static int getReqCount();

    static bool isAOK();

    static void Reset();

    static void setGo(bool go);

    static void setRName(std::string const& rname) {_myRName = rname;}

private:
    static std::string _myRName;
};

}}} // namespace

#endif
