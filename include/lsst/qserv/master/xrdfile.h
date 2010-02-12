#ifndef LSST_QSERV_MASTER_XRDFILE_H
#define LSST_QSERV_MASTER_XRDFILE_H

#include <string>

class XrdPosixCallBack; // Forward.

namespace lsst {
namespace qserv {
namespace master {

struct XrdTransResult {
    int open;
    int queryWrite;
    int read;
    int localWrite;  
};
	
void xrdInit(); // Perform some library initialization (call only once).

int xrdOpen(const char *path, int oflag);
int xrdOpenAsync(const char* path, int oflag, XrdPosixCallBack *cbP);

long long xrdRead(int fildes, void *buf, unsigned long long nbyte);

long long xrdWrite(int fildes, const void *buf, unsigned long long nbyte);

int xrdClose(int fildes);

long long xrdLseekSet(int fildes, unsigned long long offset);

std::string xrdGetEndpoint(int fildes);

int xrdReadStr(int fildes, char *str, int len);

void xrdReadToLocalFile(int fildes, int fragmentSize, 
                        const char* filename, 
                        int* write, int* read);

XrdTransResult xrdOpenWriteReadSaveClose(const char *path, 
                                         const char* buf, int len, 
                                         int fragmentSize,
                                         const char* outfile);
XrdTransResult xrdOpenWriteReadSave(const char *path, 
                                    const char* buf, int len, 
                                    int fragmentSize,
                                    const char* outfile);


}}} // namespace lsst::qserv::master

#endif // #ifndef LSST_QSERV_MASTER_XRDFILE_H
