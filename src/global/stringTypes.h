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
#ifndef LSST_QSERV_STRINGTYPES_H
#define LSST_QSERV_STRINGTYPES_H
/**
 * @brief  Global string types
 *
 */

#include <map>
#include <string>
#include <vector>

namespace lsst::qserv {
typedef std::map<std::string, std::string> StringMap;
typedef std::map<std::string, StringMap> StringMapMap;
typedef std::pair<std::string, std::string> StringPair;
typedef std::vector<StringPair> StringPairVector;
typedef std::vector<std::string> StringVector;

}  // namespace lsst::qserv
#endif  // LSST_QSERV_STRINGTYPES_H
