/*
 * LSST Data Management System
 * Copyright 2009-2013 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_TRANSACTION_H
#define LSST_QSERV_MASTER_TRANSACTION_H
/**
  * @file transaction.h
  *
  * @brief Value classes for SWIG-mediated interaction between Python
  * and C++. Includes TransactionSpec.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <string>
#include <vector>
#include <list>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {
/// class TransactionSpec - A value class for the minimum
/// specification of a subquery, as far as the xrootd layer is
/// concerned.
class TransactionSpec {
public:
 TransactionSpec() : chunkId(-1) {}
    int chunkId;
    std::string path;
    std::string query;
    int bufferSize;
    std::string savePath;

    bool isNull() const { return path.length() == 0; }

    class Reader;  // defined in thread.h
};
}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_TRANSACTION_H
