#ifndef LSST_QSERV_MASTER_SUBSTITUTION_H
#define LSST_QSERV_MASTER_SUBSTITUTION_H

#include <string>

namespace lsst {
namespace qserv {
namespace master {

class Substitution {
public:
    typedef std::pair<std::string, std::string> StringPair;
    typedef std::map<std::string, std::string> Mapping;
    typedef std::vector<StringPair> MapVector;
    typedef std::vector<std::string> StringVector;

    Substitution(std::string template_, std::string const& delim, bool shouldFinalize=true);
    
    std::string transform(Mapping const& m);

private:
    struct Item { //where subs are needed and how big the placeholders are.
	std::string name;
	int position;
	int length;
    };

    template<typename Iter> 
    inline unsigned _max(Iter begin, Iter end) {
	unsigned m=0;
	for(Iter i = begin; i != end; ++i) {
	    if(m < i->size()) m = i->size();
	}
	return m;
    }

    void _build(std::string const& delim);

    std::vector<Item> _index;
    std::string _template;
    bool _shouldFinalize;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_SUBSTITUTION_H

