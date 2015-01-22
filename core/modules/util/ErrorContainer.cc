/*
 * ErrorContainer.cpp
 *
 *  Created on: Jan 21, 2015
 *      Author: qserv
 */

#include "ErrorContainer.h"


// System headers
#include <iterator>
#include <sstream>

namespace lsst {
namespace qserv {
namespace util {

template<typename Error>
ErrorContainer<Error>::ErrorContainer() {
    // TODO Auto-generated constructor stub

}

template<typename Error>
ErrorContainer<Error>::~ErrorContainer() {
    // TODO Auto-generated destructor stub
}

template<typename Error>
void ErrorContainer<Error>::push(Error const& error) {
    // append copy of passed element
    _errors.push_back(error);
}

template<typename Error>
std::string ErrorContainer<Error>::toString() const {
    std::ostringstream oss;

     oss << "[";
     if (!_errors.empty())
     {
       // Convert all but the last element to avoid a trailing ","
       std::copy(_errors.begin(), _errors.end()-1,
           std::ostream_iterator<Error>(oss, ","));

       // Now add the last element with no delimiter
       oss << _errors.back();
     }
     oss << "]";
     return oss.str();
}

}
}
} // lsst::qserv::util
