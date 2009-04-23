#ifndef LSST_QSERV_MASTER_XRDFILE_H
#define LSST_QSERV_MASTER_XRDFILE_H

namespace lsst {
namespace qserv {
namespace master {
	
int xrdOpen(const char *path, int oflag);

long long xrdRead(int fildes, void *buf, unsigned long long nbyte);

long long xrdWrite(int fildes, const void *buf, unsigned long long nbyte);

int xrdClose(int fildes);

}}} // namespace lsst::qserv::master

#endif // #ifndef LSST_QSERV_MASTER_XRDFILE_H
