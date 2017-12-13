// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @brief Interface to the Common State System - in memory key-value based
  * implementation.
  *
  * @Author Jacek Becla, SLAC
  */

#ifndef LSST_QSERV_CSS_KVINTERFACEIMPLMEM_H
#define LSST_QSERV_CSS_KVINTERFACEIMPLMEM_H

// System headers
#include <map>
#include <memory>
#include <string>
#include <vector>

// Local headers
#include "css/KvInterface.h"


namespace lsst {
namespace qserv {
namespace css {

class KvInterfaceImplMem : public KvInterface {
public:
    explicit KvInterfaceImplMem(bool readOnly=false) : _readOnly(readOnly) {}
    explicit KvInterfaceImplMem(std::istream& mapStream, bool readOnly=false);
    explicit KvInterfaceImplMem(std::string const& filename, bool readOnly=false);

    virtual ~KvInterfaceImplMem();

    virtual std::string create(std::string const& key, std::string const& value,
                               bool unique=false) override;
    virtual void set(std::string const& key, std::string const& value) override;
    virtual bool exists(std::string const& key) override;
    virtual std::map<std::string, std::string> getMany(std::vector<std::string> const& keys) override;
    virtual std::vector<std::string> getChildren(std::string const& key) override;
    virtual std::map<std::string, std::string> getChildrenValues(std::string const& key) override;
    virtual void deleteKey(std::string const& key) override;
    virtual std::string dumpKV(std::string const& key=std::string()) override;

    std::shared_ptr<KvInterfaceImplMem> clone() const;

protected:
    virtual std::string _get(std::string const& key,
                             std::string const& defaultValue,
                             bool throwIfKeyNotFound) override;

private:
    void _init(std::istream& mapStream);
    std::map<std::string, std::string> _kvMap;
    bool _readOnly = false;
};

}}} // namespace lsst::qserv::css

#endif // LSST_QSERV_CSS_INTERFACEIMPLMEM_H
