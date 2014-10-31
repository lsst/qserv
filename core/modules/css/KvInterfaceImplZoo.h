// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file
  *
  * @brief Interface to the Common State System - zookeeper-based implementation.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_INTERFACEIMPLZOO_H
#define LSST_QSERV_CSS_INTERFACEIMPLZOO_H

// System headers
#include <vector>
#include <string>

// Third-party headers
#include "boost/thread/mutex.hpp"
#include "zookeeper/zookeeper.h"

// Local headers
#include "css/KvInterface.h"

namespace lsst {
namespace qserv {
namespace css {

class KvInterfaceImplZoo : public KvInterface {
public:
    KvInterfaceImplZoo(std::string const& connInfo, int timeout_msec);
    virtual ~KvInterfaceImplZoo();

    virtual void create(std::string const& key, std::string const& value);
    virtual void set(std::string const& key, std::string const& value);
    virtual bool exists(std::string const& key);
    virtual std::vector<std::string> getChildren(std::string const& key);
    virtual void deleteKey(std::string const& key);

protected:
    virtual std::string _get(std::string const& key,
                             std::string const& defaultValue,
                             bool throwIfKeyNotFound);

private:
    void _doConnect();
    void _disconnect();
    void _throwZooFailure(int, std::string const& fName, std::string const& key);

private:
    boost::mutex _mutex;
    zhandle_t *_zh; ///< zhookeeper handle
    const std::string _connInfo;
    int _timeout;   ///< in millisec
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_INTERFACEIMPLZOO_H
