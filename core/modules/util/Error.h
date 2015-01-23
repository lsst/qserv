/*
 * Error.h
 *
 *  Created on: Jan 22, 2015
 *      Author: qserv
 */

#ifndef UTIL_ERROR_H_
#define UTIL_ERROR_H_

// System headers
#include <string>

namespace lsst {
namespace qserv {
namespace util {

class Error {
public:
    Error(int code, std::string msg) :
            code(code), msg(msg) {
    }
    virtual ~Error() {
    }
    std::string toString() const;

    int code;
    std::string msg;

    friend std::ostream& operator<<(std::ostream &out, Error const& error);
};

}
}
} // lsst::qserv::util

#endif /* UTIL_ERROR_H_ */
