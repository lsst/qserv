#ifndef LSST_LSPEED_MYSQLFSDIRECTORY_H
#define LSST_LSPEED_MYSQLFSDIRECTORY_H

#include "XrdSfs/XrdSfsInterface.hh"

namespace lsst {
namespace qserv {
namespace worker {

class MySqlFsDirectory : public XrdSfsDirectory {
public:
    MySqlFsDirectory(char* user = 0);
    ~MySqlFsDirectory(void);

    int open(char const* dirName, XrdSecEntity const* client = 0,
             char const* opaque = 0);
    char const* nextEntry(void);
    int close(void);
    char const* FName(void);
};

}} // namespace lsst::qserv::worker

#endif
