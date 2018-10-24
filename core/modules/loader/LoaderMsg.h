// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_LOADERMSG_H_
#define LSST_QSERV_LOADER_LOADERMSG_H_

// system headers
#include <arpa/inet.h>
#include <cstring>
#include <memory>
#include <string>

// Qserv headers
#include "loader/BufferUdp.h"
#include "proto/ProtoImporter.h"


#define MAX_MSG_STRING_LENGTH 5000


namespace lsst {
namespace qserv {
namespace loader {


// Expand to include basic message information &&&
class LoaderMsgErr : public std::exception {
public:
    LoaderMsgErr(std::string const& msg, std::string const& file, int line) {
        _msg = msg + " " + file + ":" + std::to_string(line);
    }
    LoaderMsgErr(std::string const& msg) : _msg(msg) {}
    const char* what() const throw() override {
        return _msg.c_str();
    }
private:
    std::string _msg;
};


// Base class for message elements
class MsgElement {
public:
    using Ptr = std::shared_ptr<MsgElement>;
    enum {
        NOTHING     = 0,
        STRING_ELEM = 1,
        UINT16_ELEM = 2,
        UINT32_ELEM = 3,
        UINT64_ELEM = 4
    };

    explicit MsgElement(char elementType) : _elementType(elementType) {}
    MsgElement() = delete;
    MsgElement(MsgElement const&) = delete;
    MsgElement& operator=(MsgElement const&) = delete;
    virtual  ~MsgElement() = default;

    virtual bool appendToData(BufferUdp& data)=0;
    virtual bool retrieveFromData(BufferUdp &data)=0;

    /// Return the TRANSMITTED size of the element. For StringElement, this is not know until
    /// the string has been constructed. Numeric elements have constant size.
    virtual size_t transmitSize() const =0;

    static MsgElement::Ptr create(char elementType);

    static bool retrieveType(BufferUdp &data, char& elemType);

    static MsgElement::Ptr retrieve(BufferUdp& data);


    /* &&&
    static bool retrieveType(BufferUdp &data, char& elemType) {
        return data.retrieve(&elemType, sizeof(elemType));
    }

    static MsgElement::Ptr retrieve(BufferUdp& data) {
        char elemT;
        if (not retrieveType(data, elemT)) {
            return nullptr;
        }
        MsgElement::Ptr msgElem = create(elemT);
        if (msgElem != nullptr && not msgElem->retrieveFromData(data)) {
            // No good way to recover from missing data from a know type.
            throw LoaderMsgErr("static retrieve, incomplete data for type=" +
                    std::to_string((int)elemT) + " data:" + data.dump());
        }
        return msgElem;
    } */

    static bool equal(MsgElement* a, MsgElement* b) {
        if (a == b) return true;
        if (a == nullptr || b == nullptr) return false;
        if (a->_elementType != b->_elementType) { return false; }
        if (a->_elementType == NOTHING) { return true; }
        return a->equal(b);
    }

    char getElementType() const { return _elementType; }

    static std::string getStringVal(MsgElement::Ptr const& msgElem) {
        if (msgElem == nullptr) return std::string("nullptr");
        return msgElem->getStringVal();
    }

    virtual std::string getStringVal()=0;

    virtual bool equal(MsgElement* other)=0;

    size_t sizeOfBase() const { return sizeof(_elementType); }

protected:
    bool _appendType(BufferUdp &data) const {
        return data.append(&_elementType, sizeof(_elementType));
    }


    const char* _retrieveType(const char* data) {
        _elementType = *data;
        const char* ptr = data + 1;
        return ptr;
    }

private:
    char _elementType{NOTHING};
};


// Generic numeric type for network transmission.
template <class T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
class NumElement : public MsgElement {
public:
    explicit NumElement(char myType) : MsgElement(myType) {}
    NumElement(char myType, T element_) : MsgElement(myType), element(element_) {}
    NumElement() = delete;
    NumElement(NumElement<T> const&) = delete;
    NumElement& operator=(NumElement<T> const&) = delete;

    bool appendToData(BufferUdp &data) {
        if (_appendType(data)) {
            T netElem = changeEndianessOnLittleEndianOnly(element);
            return data.append(&netElem, _sizeT);
        }
        return false;
    }


    bool retrieveFromData(BufferUdp &data) override {
        T netElem;
        if (data.retrieve(&netElem, sizeof(T))) {
            element = changeEndianessOnLittleEndianOnly(netElem);
            return true;
        }
        return false;
    }

    size_t transmitSize() const override {
        return _sizeT + sizeOfBase();
    }

    std::string getStringVal() { return std::to_string(element); }

    T element{0};

    // This function will change endianess only on little endian machines, which should be
    // effective for big-endian transformation for network communication.
    T changeEndianessOnLittleEndianOnly(T const& in) {
        uint8_t data[_sizeT];
        memcpy(&data, &in, _sizeT);
        T res = 0;
        int shift = 0;
        int pos = _sizeT -1;
        for (size_t j=0; j < _sizeT; ++j) {
            res |= static_cast<T>(data[pos]) << shift;
            shift += 8;
            --pos;
        }
        return res;
    }


    bool equal(MsgElement* other) override {
        NumElement<T>* ptr = dynamic_cast<NumElement<T>*>(other);
        if (ptr == nullptr) { return false; }
        return (element == ptr->element);
    }

private:
    static const size_t _sizeT{sizeof(T)};
};


class UInt16Element : public NumElement<uint16_t> {
public:
    using Ptr = std::shared_ptr<UInt16Element>;
    static const int MYTYPE = UINT16_ELEM;

    UInt16Element() : NumElement(MYTYPE) {}
    explicit UInt16Element(uint16_t element_) : NumElement(MYTYPE, element_) {}
    UInt16Element(UInt16Element const&) = delete;
    UInt16Element& operator=(UInt16Element const&) = delete;
};


class UInt32Element : public NumElement<uint32_t> {
public:
    using Ptr = std::shared_ptr<UInt32Element>;
    static const int MYTYPE = UINT32_ELEM;

    UInt32Element() : NumElement(MYTYPE) {}
    explicit UInt32Element(uint32_t element_) : NumElement(MYTYPE, element_) {}
    UInt32Element(UInt32Element const&) = delete;
    UInt32Element& operator=(UInt32Element const&) = delete;
};


class UInt64Element : public NumElement<uint64_t> {
public:
    using Ptr = std::shared_ptr<UInt64Element>;
    static const int MYTYPE = UINT64_ELEM;

    UInt64Element() : NumElement(MYTYPE) {}
    explicit UInt64Element(uint64_t element_) : NumElement(MYTYPE, element_) {}
    UInt64Element(UInt64Element const&) = delete;
    UInt64Element& operator=(UInt64Element const&) = delete;
};



// Using uint16_t for length in all cases.
class StringElement : public MsgElement {
public:
    using Ptr = std::shared_ptr<StringElement>;
    using UPtr = std::unique_ptr<StringElement>;
    static const int myType = STRING_ELEM;

    StringElement(std::string const& element_) : MsgElement(myType), element(element_) {}
    StringElement() : MsgElement(myType) {}
    StringElement(StringElement const&) = delete;
    StringElement& operator=(StringElement const&) = delete;

    ~StringElement() override = default;

    bool appendToData(BufferUdp& data) override;
    bool retrieveFromData(BufferUdp& data) override;
    std::string getStringVal() override { return element; }

    std::string element;

    bool equal(MsgElement* other) override {
        StringElement* ptr = dynamic_cast<StringElement*>(other);
        if (ptr == nullptr) { return false; }
        return (ptr->element == ptr->element);
    }

    /// The size of StringElement changes!
    size_t transmitSize() const override {
        // char in string, variable to transmit string length, size of base class.
        return element.size()+ sizeof(uint16_t) + sizeOfBase();
    }

    template<typename T>
    std::unique_ptr<T> protoParse() {
        std::unique_ptr<T> protoItem(new T());
        bool success = proto::ProtoImporter<T>::setMsgFrom(*protoItem, element.data(), element.length());
        if (not success) {
            return nullptr;
        }
        return protoItem;
    }


    template<typename T>
    static std::unique_ptr<T> protoParse(BufferUdp& data) {
        StringElement::Ptr itemData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(data));
        if (itemData == nullptr) { return nullptr; }
        return itemData->protoParse<T>();
    }
};


/// Base class for loader messages.
//  These messages are meant to be short and simple UDP messages. Long messages
//  may have difficulty being transmitted successfully.
//
//  The message contains the message kind and the address of the entity sending
//  the message.
//
class LoaderMsg {
public:
    enum {
        WAITING      = 0,
        MSG_RECEIVED = 100,    // Standard success/error response to received message.
        TEST,                  // Communications test.
        MAST_INFO_REQ,         // Request some information about the master
        MAST_INFO,             // Information about the master
        MAST_WORKER_LIST_REQ,  // Request a list of workers from the master.
        MAST_WORKER_LIST,      // List of all workers known by the master.
        MAST_WORKER_INFO_REQ,  // Request information for a single worker.
        MAST_WORKER_INFO,      // All the information the master has about one worker. TODO add key list information
        MAST_WORKER_ADD_REQ,   // Request the Master add the worker. MSG_RECIEVED + MAST_WORKER_INFO
        WORKER_KEYS_INFO_REQ,  // Master asking a worker for information about its key-value pairs.
        WORKER_KEYS_INFO,      // Information about number of key values, range, number of new keys.
        KEY_INSERT_REQ,        // Insert a new key with info. MSG_RECEIVED + KEY_INFO
        KEY_INSERT_COMPLETE,   // Key has been inserted and logged.
        KEY_INFO_REQ,          // Request info for a single key.
        KEY_INFO,              // Information about a specific key. (includes file id and row)
        WORKER_LEFT_NEIGHBOR,  // Mast assigns a left neighbor to a worker.
        WORKER_RIGHT_NEIGHBOR, // Mast assigns a right neighbor to a worker.
        IM_YOUR_L_NEIGHBOR,    // Worker message to other worker to setup being neighbors.
        IM_YOUR_R_NEIGHBOR,    // Worker message to other worker to setup being neighbors.
        NEIGHBOR_VERIFIED,     //
        SHIFT_TO_RIGHT,
        SHIFT_TO_RIGHT_RECEIVED,
        SHIFT_TO_RIGHT_COMPLETE,
        SHIFT_FROM_RIGHT,
        SHIFT_FROM_RIGHT_RECEIVED
    };

    enum {
        STATUS_SUCCESS = 0,
        STATUS_PARSE_ERR
    };

    LoaderMsg() = default;
    /// Contains the address of entity sending the message.
    LoaderMsg(uint16_t kind, uint64_t id, std::string const& host, uint32_t port);
    LoaderMsg(LoaderMsg const&) = delete;
    LoaderMsg& operator=(LoaderMsg const&) = delete;

    virtual ~LoaderMsg() = default;

    void parseFromData(BufferUdp& data);
    void serializeToData(BufferUdp& data);

    std::string getStringVal() const;

    size_t getExpectedSize() const {
        size_t exp = sizeof(msgKind->element);
        exp += sizeof(msgId->element);
        exp += senderHost->element.size();
        exp += sizeof(senderPort->element);
        return exp;
    }

    UInt16Element::Ptr msgKind;
    UInt64Element::Ptr msgId;
    StringElement::Ptr senderHost;
    UInt32Element::Ptr senderPort;

    friend std::ostream& operator<<(std::ostream& os, LoaderMsg const& loaderMsg);
};

}}} // namespace lsst::qserv::loader

#endif /* LSST_QSERV_LOADER_LOADERMSG_H_ */
