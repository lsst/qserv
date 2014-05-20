// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
#include "proto/TaskMsgDigest.h"

#include "worker.pb.h"
#ifdef __SUNPRO_CC
#include <sys/md5.h>
#else // Linux?
#include <openssl/md5.h>
#endif
namespace {
    char hexChar[16] = {'0', '1', '2', '3',
                        '4', '5', '6', '7',
                        '8', '9', 'a', 'b',
                        'c', 'd', 'e', 'f'};
}

namespace lsst {
namespace qserv {
namespace proto {

std::string
hashTaskMsg(TaskMsg const& m) {
    unsigned char hashVal[MD5_DIGEST_LENGTH];
    char output[MD5_DIGEST_LENGTH*2 + 1];
    std::string str;

    m.SerializeToString(&str); // Use whole, serialized message
    MD5(reinterpret_cast<unsigned char const*>(str.data()),
        str.size(), hashVal);
    for(int i=0; i < MD5_DIGEST_LENGTH; i++) {
        output[i*2] = hexChar[(hashVal[i] >> 4) & 0x0F];
        output[i*2 + 1] = hexChar[hashVal[i] & 0x0F];
    }
    output[MD5_DIGEST_LENGTH*2] = '\0';
    return std::string(output);
}

}}} // namespace lsst::qserv::proto
