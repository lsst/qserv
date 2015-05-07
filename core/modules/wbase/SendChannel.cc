// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#include "wbase/SendChannel.h"

// System headers
#include <iostream>
#include <sstream>
#include <vector>

// Third-party headers

namespace lsst {
namespace qserv {
namespace wbase {

/// NopChannel is a NOP implementation of SendChannel for development and
/// debugging code without an XrdSsi channel.
class NopChannel : public SendChannel {
public:
    virtual bool send(char const* buf, int bufLen) {
        std::cout << "NopChannel send(" << (void*) buf
                  << ", " << bufLen << ");\n";
        return true;
    }

    virtual bool sendError(std::string const& msg, int code) {
        std::cout << "NopChannel sendError(\"" << msg
                  << "\", " << code << ");\n";
        return true;
    }
    virtual bool sendFile(int fd, Size fSize) {
        std::cout << "NopChannel sendFile(" << fd
                  << ", " << fSize << ");\n";
        return true;
    }
    virtual bool sendStream(char const* buf, int bufLen, bool last) {
        std::cout << "NopChannel sendStream(" << (void*) buf
                  << ", " << bufLen << ", "
                  << (last ? "true" : "false") << ");\n";
        return true;
    }
};

std::shared_ptr<SendChannel> SendChannel::newNopChannel() {
    std::shared_ptr<NopChannel> n = std::make_shared<NopChannel>();
    return n;
}


/// StringChannel is an almost-trivial implementation of a SendChannel that
/// remembers what it has received.
class StringChannel : public SendChannel {
public:
    StringChannel(std::string& dest) : _dest(dest) {}

    virtual bool send(char const* buf, int bufLen) {
        _dest.append(buf, bufLen);
        return true;
    }

    virtual bool sendError(std::string const& msg, int code) {
        std::ostringstream os;
        os << "(" << code << "," << msg << ")";
        _dest.append(os.str());
        return true;
    }

    virtual bool sendFile(int fd, Size fSize) {
        std::vector<char> buf(fSize);
        Size remain = fSize;
        while(remain > 0) {
            Size frag = ::read(fd, buf.data(), remain);
            if(frag < 0) {
                std::cout << "ERROR reading from fd during "
                          << "StringChannel::sendFile(" << "," << fSize << ")";
                return false;
            } else if(frag == 0) {
                std::cout << "ERROR unexpected 0==read() during "
                          << "StringChannel::sendFile(" << "," << fSize << ")";
                return false;
            }
            _dest.append(buf.data(), frag);
            remain -= frag;
        }
        release();
        return true;
    }

    virtual bool sendStream(char const* buf, int bufLen, bool last) {
        _dest.append(buf, bufLen);
        std::cout << "StringChannel sendStream(" << (void*) buf
                  << ", " << bufLen << ", "
                  << (last ? "true" : "false") << ");\n";
        return true;
    }
private:
    std::string& _dest;
};

std::shared_ptr<SendChannel> SendChannel::newStringChannel(std::string& d) {
    return std::shared_ptr<StringChannel>(new StringChannel(d));
}

}}} // namespace lsst::qserv::wbase
