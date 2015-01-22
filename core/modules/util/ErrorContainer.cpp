/*
 * ErrorContainer.cpp
 *
 *  Created on: Jan 21, 2015
 *      Author: qserv
 */

#include "ErrorContainer.h"

namespace lsst {
namespace qserv {
namespace util {

template<typename T>
ErrorContainer<T>::ErrorContainer() {
    // TODO Auto-generated constructor stub

}

template<typename T>
ErrorContainer<T>::~ErrorContainer() {
    // TODO Auto-generated destructor stub
}

template<typename T>
void ErrorContainer<T>::push(T const& error) {
    // append copy of passed element
    _errors.push_back(error);
}

template<typename T>
std::string ErrorContainer<T>::toString() const {
    std::ostringstream oss;

     oss << _errors.back() << "[";
     if (!_errors.empty())
     {
       // Convert all but the last element to avoid a trailing ","
       std::copy(_errors.begin(), _errors.end()-1,
           std::ostream_iterator<T>(oss, ","));

       // Now add the last element with no delimiter
       oss << _errors.back();
     }
     oss  << "]";
     return oss.str();
}

template<typename T>
std::ostream& operator<<(std::ostream &out,
        ErrorContainer<T> const& errorContainer) {
    // Since operator<< is a friend of the Point class, we can access
    // Point's members directly.
    out << errorContainer.toString();
    return out;
}

}
}
} // lsst::qserv::util
