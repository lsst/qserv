// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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

#include "MySqlListener.h"

namespace lsst {
namespace qserv {
namespace parser {


void MySqlListener::enterRoot(MySqlParser::RootContext * ctx) {
//    _rootComponent = std::make_shared<RootComponent>();
//    _queryComponents.push(_rootComponent);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
//    _queryComponents.pop();
}


}}} // namespace lsst::qserv::parser
