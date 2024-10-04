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
#include "replica/apps/TestAwsS3App.h"

// System headers
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

// Third-party headers
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/S3Client.h>

using namespace std;

namespace {

string const description =
        "This application for interacting with AWS S3 services."
        " The application uses AWS C++ SDK.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<TestAwsS3App> TestAwsS3App::create(int argc, char* argv[]) {
    return shared_ptr<TestAwsS3App>(new TestAwsS3App(argc, argv));
}

TestAwsS3App::TestAwsS3App(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().commands("operation", {"READ", "WRITE", "DELETE"}, _operation)
            .option("endpoint", "The S3 service endpoint (host[:port]).", _endpoint)
            .option("access-key", "The service key (for authentication/authorization).", _accessKey)
            .option("access-secret", "The service secret (for authentication/authorization).", _accessSecret)
            .option("bucket", "The S3 bucket name.", _bucket);

    parser().command("READ")
            .description("Retrieve an object from a bucket and write it into a local file.")
            .required("key", "The S3 key of the object to be retrieved.", _key)
            .required("file", "The path to the file where the content will be written.", _file);
    parser().command("WRITE")
            .description("Write a local file as an object into a bucket.")
            .required("file", "The path to the local file.", _file)
            .required("key", "The S3 key of the object where to put the file content.", _key);

    parser().command("DELETE")
            .description("Delee an object from a bucket.")
            .required("key", "The S3 key of the object to be deleted.", _key);
}

int TestAwsS3App::runImpl() {
    string const context = "TestAwsS3App::" + string(__func__) + "  ";

    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(options);

    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(Aws::String(_accessKey));
    credentials.SetAWSSecretKey(Aws::String(_accessSecret));

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "";
    clientConfig.proxyHost = "";
    clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    clientConfig.endpointOverride = _endpoint;
    clientConfig.verifySSL = false;
    clientConfig.enableHostPrefixInjection = false;
    clientConfig.followRedirects = Aws::Client::FollowRedirectsPolicy::ALWAYS;
    clientConfig.enableEndpointDiscovery = Aws::Crt::Optional<bool>(false);

    // IMPORTANT: The virtual addressing mode needs to be disabled to prevent the client
    // from attempting to connect to AWS S3 services using virtual addressing.
    bool const useVirtualAddressing = false;
    Aws::S3::S3Client client(credentials, clientConfig,
                             Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
                             useVirtualAddressing);
    int retCode = 1;
    try {
        if (_operation == "READ") {
            retCode = _readObject(client);
        } else if (_operation == "WRITE") {
            retCode = _writeObject(client);
        } else if (_operation == "DELETE") {
            retCode = _deleteObject(client);
        } else {
            cerr << context << "unsupported operation: " << _operation << endl;
        }
    } catch (exception const& e) {
        cerr << context << "failed, exception: " << e.what() << endl;
    } catch (...) {
        cerr << context << "failed, unknown exception" << endl;
    }
    Aws::ShutdownAPI(options);
    return retCode;
}

int TestAwsS3App::_readObject(Aws::S3::S3Client& client) {
    string const context = "TestAwsS3App::" + string(__func__) + "  ";
    cout << context << " downloading object " << _object2str() << " into file '" << _file << "'" << endl;

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(Aws::String(_bucket));
    request.SetKey(Aws::String(_key));

    Aws::S3::Model::GetObjectOutcome outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        _reportS3Error(context + "object downloading failed", outcome.GetError());
        return 1;
    }
    cout << context << "object downloading finished" << endl;
    Aws::IOStream& ioStream = outcome.GetResultWithOwnership().GetBody();
    Aws::OFStream outStream(_file, ios_base::out | ios_base::binary);
    if (!outStream.is_open()) {
        cerr << context << "file open/create failed" << endl;
        return 1;
    }
    outStream << ioStream.rdbuf();
    cout << context << "finished writing the object into the file" << endl;
    return 0;
}

int TestAwsS3App::_writeObject(Aws::S3::S3Client& client) {
    string const context = "TestAwsS3App::" + string(__func__) + "  ";
    cout << context << " uploading file '" << _file << "' into object " << _object2str() << endl;

    shared_ptr<Aws::FStream> const inputData =
            Aws::MakeShared<Aws::FStream>("SampleAllocationTag", _file, ios_base::in | ios_base::binary);
    if (!inputData->is_open()) {
        cerr << context << "failed to open file, '" << _file << "'." << endl;
        return 1;
    }

    Aws::String const bucket(_bucket);
    Aws::String const key(_key);
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);
    request.SetBody(inputData);

    Aws::S3::Model::PutObjectOutcome const outcome = client.PutObject(request);
    if (!outcome.IsSuccess()) {
        _reportS3Error(context + "object uploading failed", outcome.GetError());
        _deleteObjectImpl(context, client);
        return 1;
    }
    cout << "uploading finished" << endl;
    return 0;
}

int TestAwsS3App::_deleteObject(Aws::S3::S3Client& client) {
    string const context = "TestAwsS3App::" + string(__func__) + "  ";
    cout << context << "deleting object " << _object2str() << endl;
    if (_deleteObjectImpl(context, client) != 0) return 1;
    cout << "object deleted" << endl;
    return 0;
}

int TestAwsS3App::_deleteObjectImpl(string const& context, Aws::S3::S3Client& client) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(Aws::String(_bucket));
    request.SetKey(Aws::String(_key));
    Aws::S3::Model::DeleteObjectOutcome const outcome = client.DeleteObject(request);
    if (!outcome.IsSuccess()) {
        _reportS3Error(context + "object deletion failed", outcome.GetError());
        return 1;
    }
    return 0;
}

void TestAwsS3App::_reportS3Error(string const& context, Aws::S3::S3Error const& err) {
    cerr << context << ", error: " << err.GetExceptionName() << ", message: " << err.GetMessage() << endl;
}

}  // namespace lsst::qserv::replica
