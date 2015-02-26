/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

/// \file
/// \brief This file contains the BigInteger class implementation.

#include "BigInteger.h"

#include <algorithm>


namespace lsst {
namespace sg {

namespace {

// `_add` computes the sum of the digits in m1 and m2 and stores the result in
// m, which may equal either m1 or m2. It returns the number of digits in the
// sum.
unsigned _add(uint32_t * const m,
              uint32_t const * const m1,
              uint32_t const * const m2,
              unsigned const size1,
              unsigned const size2)
{
    uint64_t sum = 0;
    unsigned i = 0;
    unsigned const size = std::min(size1, size2);
    for (; i < size; ++i) {
        sum = static_cast<uint64_t>(m1[i]) + (sum >> 32) +
              static_cast<uint64_t>(m2[i]);
        m[i] = static_cast<uint32_t>(sum);
    }
    for (; i < size1; ++i) {
        sum = static_cast<uint64_t>(m1[i]) + (sum >> 32);
        m[i] = static_cast<uint32_t>(sum);
    }
    for (; i < size2; ++i) {
        sum = static_cast<uint64_t>(m2[i]) + (sum >> 32);
        m[i] = static_cast<uint32_t>(sum);
    }
    uint32_t carry = static_cast<uint32_t>(sum >> 32);
    if (carry != 0) {
        m[i] = carry;
        ++i;
    }
    return i;
}

// `_sub` computes the difference of the digits in m1 and m2 and stores the
// result in m, which may equal either m1 or m2. It assumes that the difference
// is non-negative, and returns the the number of digits in the difference.
unsigned _sub(uint32_t * const m,
              uint32_t const * const m1,
              uint32_t const * const m2,
              unsigned const size1,
              unsigned const size2)
{
    int64_t diff = 0;
    unsigned i = 0;
    // Note that right shifting a negative integer is implementation defined
    // (not undefined) behavior. Here we assume arithmetic right shifts for
    // negative integers.
    //
    // TODO(smm): check for this at build time, and provide an alternate
    //            implementation when this assumption does not hold.
    for (; i < size2; ++i) {
        diff = static_cast<int64_t>(m1[i]) + (diff >> 32) -
               static_cast<int64_t>(m2[i]);
        m[i] = static_cast<uint32_t>(diff);
    }
    // Borrow from the remaining digits in m1 while necessary.
    for (; diff < 0; ++i) {
        diff = static_cast<int64_t>(m1[i]) - 1;
        m[i] = static_cast<uint32_t>(diff);
    }
    // If we subtracted anything from the most significant digit in m1, strip
    // any leading zeroes that may have been introduced.
    if (i == size1) {
        while (m1[i - 1] == 0) { --i; }
        return i;
    }
    // Otherwise, copy the remaining digits from m1 to m.
    for (; i < size1; ++i) {
        m[i] = m1[i];
    }
    return i;
}

// `_mul` computes the product of m1 and m2 and stores the result in m,
// which may equal either m1 or m2. It assumes that m1 has at least as many
// digits as m2, and returns the the number of digits in the product.
unsigned _mul(uint32_t * const m,
              uint32_t const * const m1,
              uint32_t const * const m2,
              unsigned const size1,
              unsigned const size2)
{
    // The algorithm implemented is long multiplication, and is good for small
    // digit counts. A requirement for anything fancier involves enough
    // complexity that switching to GMP is definitely warranted.
    //
    // The only small subtlety here is that because m is allowed to alias m1
    // or m2, we compute products of m2 from most to least significant digit of
    // m1. This way no as yet unprocessed input digits are overwritten.
    unsigned const size = size1 + size2;
    for (unsigned i = size1; i < size; ++i) {
        m[i] = 0;
    }
    for (unsigned i = size1; i > 0; --i) {
        uint64_t digit = m1[i - 1];
        uint64_t carry = static_cast<uint64_t>(m2[0]) * digit;
        unsigned j = 1;
        m[i - 1] = static_cast<uint32_t>(carry);
        carry >>= 32;
        for (; j < size2; ++j) {
            carry = static_cast<uint64_t>(m2[j]) * digit +
                    static_cast<uint64_t>(m[i + j - 1]) +
                    carry;
            m[i + j - 1] = static_cast<uint32_t>(carry);
            carry >>= 32;
        }
        for (; carry != 0; ++j) {
            carry += static_cast<uint64_t>(m[i + j - 1]);
            m[i + j - 1] = static_cast<uint32_t>(carry);
            carry >>= 32;
        }
    }
    // The product of m1 and m2 contains size1 + size2 or size1 + size2 - 1
    // digits.
    return m[size - 1] == 0 ? size - 1 : size;
}

} // unnamed namespace


BigInteger & BigInteger::add(BigInteger const & b) {
    if (b._sign == 0) {
        return *this;
    }
    if (_sign == 0) {
        *this = b;
        return *this;
    }
    if (this == &b) {
        return multiplyPow2(1);
    }
    // When adding two magnitudes, the maximum number of bits in the result is
    // one greater than the number of bits B in the larger input. When
    // subtracting them, the maximum result size is B.
    _checkCapacity(std::max(_size, b._size) + 1);
    if (_sign == b._sign) {
        // If the signs of both integers match, add their magnitudes.
        _size = _add(_digits, _digits, b._digits, _size, b._size);
        return *this;
    }
    // Otherwise, subtract the smaller magnitude from the larger. The sign of
    // the result is the sign of the input with the larger magnitude.
    if (_size > b._size) {
        _size = _sub(_digits, _digits, b._digits, _size, b._size);
    } else if (_size < b._size) {
        _size = _sub(_digits, b._digits, _digits, b._size, _size);
        _sign = b._sign;
    } else {
        // Both magnitudes have the same number of digits. Compare and discard
        // leading common digits until we find a digit position where the
        // magnitudes differ, or we determine that they are identical.
        int i = _size;
        for (; i > 0 && _digits[i - 1] == b._digits[i - 1]; --i) {}
        if (i == 0) {
            setToZero();
        } else if (_digits[i - 1] > b._digits[i - 1]) {
            _size = _sub(_digits, _digits, b._digits, i, i);
        } else {
            _size = _sub(_digits, b._digits, _digits, i, i);
            _sign = b._sign;
        }
    }
    return *this;
}

BigInteger & BigInteger::subtract(BigInteger const & b) {
    if (this != &b) {
        // Avoid code duplication by computing a - b = -(-a + b).
        // This only works if a and b are distinct.
        negate();
        add(b);
        negate();
    } else {
        setToZero();
    }
    return *this;
}

BigInteger & BigInteger::multiplyPow2(unsigned n) {
    if (_sign == 0 || n == 0) {
        return *this;
    }
    // Decompose n into (s, z), where s is a shift by less than 32 bits, and
    // z is the number of trailing zero digits introduced by the shift.
    unsigned const z = (n >> 5);
    unsigned const s = (n & 0x1f);
    unsigned const size = _size + z;
    _checkCapacity(size + 1);
    if (s == 0) {
        // Right-shifting an unsigned 32 bit integer by 32 bits is undefined
        // behavior. Avoid that using this special case code.
        for (unsigned i = _size; i != 0; --i) {
            _digits[i + z - 1] = _digits[i - 1];
        }
        for (unsigned i = z; i != 0; --i) {
            _digits[i - 1] = 0;
        }
        _size = size;
    } else {
        uint32_t low, high = 0;
        for (unsigned i = _size; i != 0; --i, high = low) {
            low = _digits[i - 1];
            _digits[i + z] = (high << s) | (low >> (32 - s));
        }
        _digits[z] = high << s;
        for (unsigned i = z; i != 0; --i) {
            _digits[i] = 0;
        }
        _size = (_digits[size] == 0) ? size: size + 1;
    }
    return *this;
}

BigInteger & BigInteger::multiply(BigInteger const & b) {
    _sign *= b._sign;
    if (_sign == 0) {
        _size = 0;
        return *this;
    }
    _checkCapacity(_size + b._size);
    if (_size >= b._size) {
        _size = _mul(_digits, _digits, b._digits, _size, b._size);
    } else {
        _size = _mul(_digits, b._digits, _digits, b._size, _size);
    }
    return *this;
}

}} // namespace lsst::sg
