// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2014 LSST Corporation.
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

#ifndef LSST_QSERV_QDISP_TRANSACTIONSPEC_H
#define LSST_QSERV_QDISP_TRANSACTIONSPEC_H
/**
  * @file
  *
  * @brief Value classes for SWIG-mediated interaction between Python
  * and C++. Includes TransactionSpec.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>
#include <vector>

// Third-party headers
#include <memory>

namespace lsst {
namespace qserv {
namespace qdisp {

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

    class Reader;
};

/// class TransactionSpec::Reader
/// Constructs a TransactionSpec from an input file. Used for replaying
/// transactions during development, debugging, and load testing. Probably
/// obsolete.
class TransactionSpec::Reader {
public:
    Reader(std::string const& inFile);
    ~Reader();
    TransactionSpec getSpec();
private:
    void _readWholeFile(std::string const& inFile);
    void _setupMmap(std::string const& inFile);
    void _cleanupMmap();
    void _advanceMmap();

    char* _rawContents;
    char* _mmapChunk;
    int _mmapFd;
    int _mmapOffset;
    int _mmapChunkSize;
    int _mmapDefaultSize;
    int _mmapMinimum;
    int _rawLength;
    int _pos;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_TRANSACTIONSPEC_H
