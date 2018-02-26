#include "replica/CmdParser.h"
#if 0
#include "XrdCl/XrdClFile.hh"
#endif
#include <memory>       // std::unique_ptr
#include <iostream>
#include <stdexcept>
#include <string>

namespace rc  = lsst::qserv::replica;

namespace {

// Command line parameters

std::string  inFileUrl;
std::string outFileUrl;

uint32_t recordSizeBytes;

bool progressReport;


int test () {

#if 0
    // Data buffer to be allocated for the specified record size
    std::unique_ptr<char> recordPtr (new char[recordSizeBytes]);

    XrdCl::XRootDStatus status;

    // Open the input file.

    XrdCl::File inFile;                         
    status = inFile.Open (
        inFileUrl,
        XrdCl::OpenFlags::Flags::Read |
        XrdCl::OpenFlags::Flags::SeqIO);
    if (!status.IsOK()) {
        std::cerr << status.ToString() << std::endl;
        return status.GetShellCode();
    }

    // Create the output file.

    XrdCl::File outFile;                         
    status = outFile.Open (
        outFileUrl,
        XrdCl::OpenFlags::Flags::New |
        XrdCl::OpenFlags::Flags::SeqIO,
        XrdCl::Access::Mode::UR |
        XrdCl::Access::Mode::UW);
    if (!status.IsOK()) {
        std::cerr << status.ToString() << std::endl;
        return status.GetShellCode();
    }

    // Copy records from the input file and write them into the output
    // one.

    uint64_t offset=0;
    while (true) {
        uint32_t bytesRead = 0;
        status = inFile.Read (offset, recordSizeBytes, recordPtr.get(), bytesRead);
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetShellCode();
        }
        if (!bytesRead) break;
        status = outFile.Write(offset, bytesRead, recordPtr.get());
        if (!status.IsOK()) {
            std::cerr << status.ToString() << std::endl;
            return status.GetShellCode();
        }
        offset += bytesRead;
    }
    inFile.Close();
    outFile.Sync();
    outFile.Close();
#endif
    return 0;
}
} // namespace

int main (int argc, const char* const argv[]) {

    // Parse command line parameters
    try {
        rc::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <inFileUrl> <outFileUrl> [--record-size=<bytes>] [--progress-report]\n"
            "\n"
            "Parameters:\n"
            "  <inFileUrl>        - the logical URL of an input file to be copied\n"
            "  <outFileUrl>       - the logical URL of an output destination\n"
            "\n"
            "Flags and options:\n"
            "  --record-size      - override the default record size of 1048576 bytes (1 MB)\n"
            "  --progress-report  - turn on the progress reports while copying files\n");

        ::inFileUrl       = parser.parameter<std::string> (1);
        ::outFileUrl      = parser.parameter<std::string> (2);
    
        ::recordSizeBytes = parser.option<int> ("record-size", 1048576);
        ::progressReport  = parser.flag        ("progress-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test ();
}
