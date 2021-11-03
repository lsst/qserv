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
#include "replica/IngestConfigTypes.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

string const HttpFileReaderConfig::category = "worker-http-file-reader";

string const HttpFileReaderConfig::sslVerifyHostKey = "SSL_VERIFYHOST";
string const HttpFileReaderConfig::sslVerifyPeerKey = "SSL_VERIFYPEER";
string const HttpFileReaderConfig::caPathKey    = "CAPATH";
string const HttpFileReaderConfig::caInfoKey    = "CAINFO";
string const HttpFileReaderConfig::caInfoValKey = "CAINFO_VAL";

string const HttpFileReaderConfig::proxySslVerifyHostKey = "PROXY_SSL_VERIFYHOST";
string const HttpFileReaderConfig::proxySslVerifyPeerKey = "PROXY_SSL_VERIFYPEER";
string const HttpFileReaderConfig::proxyCaPathKey    = "PROXY_CAPATH";
string const HttpFileReaderConfig::proxyCaInfoKey    = "PROXY_CAINFO";
string const HttpFileReaderConfig::proxyCaInfoValKey = "PROXY_CAINFO_VAL";

string const HttpFileReaderConfig::connectTimeoutKey = "CONNECTTIMEOUT";
string const HttpFileReaderConfig::timeoutKey        = "TIMEOUT";
string const HttpFileReaderConfig::lowSpeedLimitKey  = "LOW_SPEED_LIMIT";
string const HttpFileReaderConfig::lowSpeedTimeKey   = "LOW_SPEED_TIME";

}}}  // namespace lsst::qserv::replica
