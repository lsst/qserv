/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
#ifndef LSST_QSERV_MASTER_MERGETYPES_H
#define LSST_QSERV_MASTER_MERGETYPES_H
#include <string>

namespace lsst {
namespace qserv {
namespace master {

class MergeFixup {
public:
    MergeFixup(std::string select_,
               std::string post_,
               std::string orderBy_,
               int limit_, 
               bool needsFixup_) 
        : select(select_), post(post_),
          orderBy(orderBy_), limit(-1), 
          needsFixup(needsFixup_)
    {}
    MergeFixup() : limit(-1), needsFixup(false) {}

    std::string select;
    std::string post;
    std::string orderBy;
    int limit;
    bool needsFixup;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_MERGETYPES_H
