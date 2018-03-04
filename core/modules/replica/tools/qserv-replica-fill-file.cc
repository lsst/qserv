#include "util/CmdLineParser.h"
#if 0
#include "XrdCl/XrdClFile.hh"
#endif
#include <memory>       // std::unique_ptr
#include <iostream>
#include <stdexcept>
#include <string>

namespace util = lsst::qserv::util;

namespace {

// Command line parameters

std::string outFileUrl;

uint32_t recordSizeBytes;
uint32_t numRecords;

bool progressReport;

/// The test
int test () {

#if 0
    // Data buffer to be allocated for the specified record size
    std::unique_ptr<char> recordPtr (new char[recordSizeBytes]);

    XrdCl::XRootDStatus status;

    // Create the file.

    XrdCl::File file;                         
    status = file.Open (
        outFileUrl,
        XrdCl::OpenFlags::Flags::New |
        XrdCl::OpenFlags::Flags::SeqIO,
        XrdCl::Access::Mode::UR |
        XrdCl::Access::Mode::UW);
    if (!status.IsOK()) {
        std::cerr << status.ToString() << std::endl;
        return status.GetShellCode();
    }
    uint64_t offset=0;
    for (uint32_t i=0; i < numRecords; ++i) {
        status = file.Write(offset, recordSizeBytes, recordPtr.get());
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            file.Close();
            return status.GetShellCode();
        }
        offset += recordSizeBytes;
        if (progressReport)
            std::cout << "file size: " <<  offset << "\n";
    }
    file.Sync();
    return file.Close().GetShellCode();
#else
    return 0;
#endif
}
} // namespace

int main (int argc, const char* const argv[]) {

    // Parse command line parameters
    try {
        util::CmdLineParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <outFileUrl> [--record-size=<bytes>] [--num-records] [--progress-report]\n"
            "\n"
            "Parameters:\n"
            "  <outFileUrl>       - the logical URL of an output destination\n"
            "\n"
            "Flags and options:\n"
            "  --record-size      - override the default record size of 1048576 bytes (1 MB)\n"
            "  --num-records      - override the default number of records wgich is equal to 1\n"
            "  --progress-report  - turn on the progress reports while writing into the file\n");

        ::outFileUrl      = parser.parameter<std::string> (1);
    
        ::recordSizeBytes = parser.option<int> ("record-size", 1048576);
        ::numRecords      = parser.option<int> ("num-records", 1);
        ::progressReport  = parser.flag        ("progress-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test ();
}
