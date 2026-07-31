/*
 * LSST Data Management System
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

// Class header
#include "ChunkUseCountAnswerMsg.h"

#include <stdexcept>

// Qserv headers
#include "http/RequestBodyJSON.h"
#include "protojson/PwHideJson.h"
#include "wpublish/QueriesAndChunks.h"

// LSST headers
#include "global/LogQ.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.ChunkUseCountAnswer");
}  // namespace

namespace lsst::qserv::protojson {

json ChunkUseCountAnswerMsg::toJson() const {
    json jsCounts = json::object();
    for (auto const& [dbName, chunkCountMap] : *_dbchunkCountMap) {
        json chunkCountJson;
        for (auto const& [chunkId, useCount] : chunkCountMap) {
            chunkCountJson[std::to_string(chunkId)] = useCount;
        }
        jsCounts[dbName] = chunkCountJson;
    }
    json js;
    js["dbChunkUseCount"] = jsCounts;
    return js;
}

ChunkUseCountAnswerMsg::Ptr ChunkUseCountAnswerMsg::createFromJson(nlohmann::json const& jsin) {
    DbChunkCountMapPtr dbchunkCountMap = make_shared<DbChunkCountMap>();
    json jsArray = jsin.at("dbChunkUseCount");
    for (auto const& [dbName, chunkCountJson] : jsArray.items()) {
        for (auto const& [chunkIdStr, useCountJson] : chunkCountJson.items()) {
            int chunkId = std::stoi(chunkIdStr);
            int useCount = useCountJson.get<int>();
            // chunkUseCountAnswerMsg->(*_dbchunkCountMap)[dbName][chunkId] = useCount;
            (*dbchunkCountMap)[dbName][chunkId] = useCount;
        }
    }
    return ChunkUseCountAnswerMsg::create(dbchunkCountMap);
}

bool ChunkUseCountAnswerMsg::equal(ChunkUseCountAnswerMsg const& other) const {
    if (_dbchunkCountMap->size() != other._dbchunkCountMap->size()) {
        return false;
    }
    for (auto const& [dbName, chunkCountMap] : *_dbchunkCountMap) {
        auto iter = other._dbchunkCountMap->find(dbName);
        if (iter == other._dbchunkCountMap->end()) {
            return false;
        }
        auto const& otherChunkCountMap = iter->second;
        if (chunkCountMap.size() != otherChunkCountMap.size()) {
            return false;
        }
        for (auto const& [chunkId, useCount] : chunkCountMap) {
            auto iter2 = otherChunkCountMap.find(chunkId);
            if (iter2 == otherChunkCountMap.end()) {
                return false;
            }
            if (useCount != iter2->second) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace lsst::qserv::protojson
