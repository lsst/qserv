#ifndef LSST_QSERV_MASTER_TRANSACTION_H
#define LSST_QSERV_MASTER_TRANSACTION_H

namespace lsst {
namespace qserv {
namespace master {

class TransactionSpec {

public:
    std::string path;
    std::string query;
    int bufferSize;
    std::string savePath;
    
    bool isNull() const { return path.length() == 0; }
    
    class Reader;  // defined in thread.h
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_TRANSACTION_H
