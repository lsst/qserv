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
#ifndef LSST_QSERV_REPLICA_TESTAWSS3APP_H
#define LSST_QSERV_REPLICA_TESTAWSS3APP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// Forward declarations
namespace Aws::S3 {
class S3Client;
class S3Error;
}  // namespace Aws::S3

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class TestAwsS3App implements a tool for interacting with AWS S3 services.
 * The application uses AWS C++ SDK.
 */
class TestAwsS3App : public Application {
public:
    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv Ahe vector of command-line arguments.
     */
    static std::shared_ptr<TestAwsS3App> create(int argc, char* argv[]);

    TestAwsS3App() = delete;
    TestAwsS3App(TestAwsS3App const&) = delete;
    TestAwsS3App& operator=(TestAwsS3App const&) = delete;

    virtual ~TestAwsS3App() override = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see TestAwsS3App::create()
    TestAwsS3App(int argc, char* argv[]);

    int _readObject(Aws::S3::S3Client& client);
    int _writeObject(Aws::S3::S3Client& client);
    int _deleteObject(Aws::S3::S3Client& client);
    int _deleteObjectImpl(std::string const& context, Aws::S3::S3Client& client);

    std::string _object2str() const { return "(key:" + _key + ",bucket:" + _bucket + ")"; }
    static void _reportS3Error(std::string const& context, Aws::S3::S3Error const& err);

    std::string _operation;
    std::string _endpoint;
    std::string _accessKey;
    std::string _accessSecret;
    std::string _bucket;
    std::string _key;
    std::string _file;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_TESTAWSS3APP_H */
