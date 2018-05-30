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
 */

// Class header
#include "replica/ChunkNumber.h"

// System headers
#include <atomic>
#include <limits>
#include <iostream>

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica {

// ------------------------------------------------------------
// ------------------- ChunkNumberValidator -------------------
// ------------------------------------------------------------

ChunkNumberValidator::ChunkNumberValidator() {
    static std::atomic<unsigned int> nextId(0);
    _id = ++nextId;
}

bool ChunkNumberValidator::operator==(ChunkNumberValidator const& rhs) const {
    return _id == rhs._id;
}

bool ChunkNumberValidator::overflow(unsigned int value) const {
    return value == overflowValue();
}

unsigned int ChunkNumberValidator::overflowValue() const {
    return 1234567890UL;
}

// -----------------------------------------------------------------------
// ------------------- ChunkNumberSingleRangeValidator -------------------
// -----------------------------------------------------------------------

ChunkNumberSingleRangeValidator::ChunkNumberSingleRangeValidator(unsigned int minValue,
                                                                 unsigned int maxValue)
    :   _minValue(minValue),
        _maxValue(maxValue) {
}

bool ChunkNumberSingleRangeValidator::valid(unsigned int value) const {
    return overflow(value) or
           ((_minValue <= value) and (value <= _maxValue));
}

// -----------------------------------------------------------------
// ------------------- ChunkNumberQservValidator -------------------
// -----------------------------------------------------------------

ChunkNumberQservValidator::ChunkNumberQservValidator(int32_t numStripes,
                                                     int32_t numSubStripesPerStripe)
    :   _chunker(numStripes, numSubStripesPerStripe) {
}

bool ChunkNumberQservValidator::valid(unsigned int value) const {
    return overflow(value) or _chunker.valid(static_cast<int32_t>(value));
}

// ---------------------------------------------------
// ------------------- ChunkNumber -------------------
// ---------------------------------------------------

ChunkNumber ChunkNumber::makeOverflow(ChunkNumberValidator::Ptr const& validator) {
    return ChunkNumber(validator->overflowValue(),
                       validator);
}

ChunkNumberValidator::Ptr const& ChunkNumber::defaultValidator() {
    static ChunkNumberValidator::Ptr const validator =
                std::make_shared<ChunkNumberSingleRangeValidator>(
                        std::numeric_limits<unsigned int>::min(),
                        std::numeric_limits<unsigned int>::max());
    return validator;
}

ChunkNumber::ChunkNumber(ChunkNumberValidator::Ptr const& validator)
    :   _valid(false),
        _overflow(false),
        _validator(validator) {
}

ChunkNumber::ChunkNumber(unsigned int value,
                         ChunkNumberValidator::Ptr const& validator)
    :   _value(value),
        _valid(validator->valid(value)),
        _overflow(validator->valid(value) and validator->overflow(value)),
        _validator(validator) {

    if (not _valid) {
        throw ChunkNumberNotValid(
                    "ChunkNumber: input number " + std::to_string(value) +
                    " can't be validated by the validator");
    }
}

unsigned int ChunkNumber::value() const {
    if (not _valid) {
        throw ChunkNumberNotValid("ChunkNumber: invalid object in a type conversion operation");
    }
    return _value;
}

bool ChunkNumber::operator==(ChunkNumber const& rhs) const {
    assertBothValid(*this, rhs);
    return _value == rhs._value;
}

bool ChunkNumber::operator<(ChunkNumber const& rhs) const {
    assertBothValid(*this, rhs);
    return _value <= rhs._value;
}

bool ChunkNumber::operator==(unsigned int value) const {
    ChunkNumber const rhs(value, _validator);
    assertBothValid(*this, rhs);
    return _value == rhs._value;
}

bool ChunkNumber::operator<(unsigned int value) const {
    ChunkNumber const rhs(value, _validator);
    assertBothValid(*this, rhs);
    return _value <= rhs._value;
}

void ChunkNumber::assertBothValid(ChunkNumber const& lhs,
                                  ChunkNumber const& rhs) {

    if (not ((*(lhs._validator) == *(rhs._validator)) and
             (lhs._valid and rhs._valid))) {

        throw ChunkNumberNotValid("ChunkNumber: invalid object(s) in a binary operation");
    }
}

// ---------------------------------------------
// ------------------- Misc. -------------------
// ---------------------------------------------

std::ostream& operator<<(std::ostream& os, ChunkNumber const& chunkNumber) {
    if (chunkNumber.valid()) os << chunkNumber.value();
    else                     os << "invalid";
    return os;
}

}}} // namespace lsst::qserv::replica
