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
#ifndef LSST_QSERV_INGESTCONFIGTYPES_H
#define LSST_QSERV_INGESTCONFIGTYPES_H

// System headers
#include <string>

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpFileReaderConfig encapculates configuration parameters related to 'libcurl'
 * option setter.
 */
class HttpFileReaderConfig {
public:
    /// The folder where the parameters are stored in the persistent configuration.
    static std::string const category;

    // ------------------------------------------------
    // Keys for the SSL certs of the final data servers
    // ------------------------------------------------

    /// A flag set with 'CURLOPT_SSL_VERIFYHOST'
    static std::string const sslVerifyHostKey;

    /// A flag set with 'CURLOPT_SSL_VERIFYPEER'
    static std::string const sslVerifyPeerKey;

    /// A path to a folder (at worker) with certs set with 'CURLOPT_CAPATH'.
    static std::string const caPathKey;

    /// A path to an existing cert file (at worker) set with 'CURLOPT_CAINFO'.
    static std::string const caInfoKey;

    /// A value of a cert which would have to be pulled from the configuration
    /// databases placed into a local file (at worker) be set with 'CURLOPT_CAINFO'.
    /// This option is used if it's impossible to preload required certificates
    /// at workers, or make them directly readable by worker's ingest services otherwise.
    static std::string const caInfoValKey;

    // --------------------------------------------------------
    // Keys for the SSL certs of the intermediate proxy servers
    // --------------------------------------------------------

    /// A flag set with 'CURLOPT_PROXY_SSL_VERIFYHOST'
    static std::string const proxySslVerifyHostKey;

    /// A flag set with 'CURLOPT_PROXY_SSL_VERIFYPEER'
    static std::string const proxySslVerifyPeerKey;

    /// A path to a folder (at worker) with certs set with 'CURLOPT_PROXY_CAPATH'.
    static std::string const proxyCaPathKey;

    /// A path to an existing cert file (at worker) set with 'CURLOPT_PROXY_CAINFO'.
    static std::string const proxyCaInfoKey;

    /// A value of a cert which would have to be pulled from the configuration
    /// databases placed into a local file (at worker) be set with 'CURLOPT_PROXY_CAINFO'.
    /// This option is used if it's impossible to preload required certificates
    /// at workers, or make them directly readable by worker's ingest services otherwise.
    static std::string const proxyCaInfoValKey;

    // Objects of this class can be trivially constructed, copied or deleted.
    // The default state of an object corresponds to not having any of the options
    // carried by the class be set when using 'libcurl' API.

    HttpFileReaderConfig() = default;
    HttpFileReaderConfig(HttpFileReaderConfig const&) = default;
    HttpFileReaderConfig& operator=(HttpFileReaderConfig const&) = default;
    ~HttpFileReaderConfig() = default;

    // Values of the parameters

    bool sslVerifyHost = true;
    bool sslVerifyPeer = true;
    std::string caPath;
    std::string caInfo;
    std::string caInfoVal;

    bool proxySslVerifyHost = true;
    bool proxySslVerifyPeer = true;
    std::string proxyCaPath;
    std::string proxyCaInfo;
    std::string proxyCaInfoVal;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_INGESTCONFIGTYPES_H
