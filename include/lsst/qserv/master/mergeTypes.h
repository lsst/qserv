#ifndef LSST_QSERV_MASTER_MERGETYPES_H
#define LSST_QSERV_MASTER_MERGETYPES_H
#include <string>

namespace lsst {
namespace qserv {
namespace master {

class MergeFixup {
public:
    MergeFixup(std::string select_,
               std::string post_,
               std::string orderBy_,
               int limit_, 
               bool needsFixup_) 
        : select(select_), post(post_),
          orderBy(orderBy_), limit(-1), 
          needsFixup(needsFixup_)
    {}
    MergeFixup() : limit(-1), needsFixup(false) {}

    std::string select;
    std::string post;
    std::string orderBy;
    int limit;
    bool needsFixup;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_MERGETYPES_H
