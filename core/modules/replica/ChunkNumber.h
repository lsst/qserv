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
#ifndef LSST_QSERV_REPLICA_CHUNKNUMBER_H
#define LSST_QSERV_REPLICA_CHUNKNUMBER_H

/**
 * This header defines a collection of classes which provide a safe abstraction
 * for chunk numbers. Objects of class ChunkNumber replace the unsigned integer
 * representation. Also, there are plugged "validator" classes which are meant
 * to restrict a range of chunk number values as well as ensure that chunk number
 * objects are used in the right context. See a description of each class for
 * further details on this subject.
 *
 * @see class ChunkNumberValidator
 * @see class ChunkNumberSingleRangeValidator
 * @see class ChunkNumberNotValid
 * @see class ChunkNumber
 */

// System headers
#include <iosfwd>
#include <memory>
#include <stdexcept>

// LSST headers
#include "lsst/sphgeom/Chunker.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ChunkNumberValidator represents an abstract interface for chunk
 * number validation services.
 *
 * Validators are tied to database families (and, indirectly, - to
 * the corresponding partitioning schemes). Validator instances are also
 * comparable. The comparison is based on the instance uniqueness.
 */
class ChunkNumberValidator {

public:

    /// Pointer type to objects of this type
    typedef std::shared_ptr<ChunkNumberValidator> Ptr;

    virtual ~ChunkNumberValidator() = default;

    /// @return 'true' if the input validator corresponds to the same instance
    bool operator==(ChunkNumberValidator const& rhs) const;

    /// @return 'true' if the input validator does not corresponds to the same instance
    bool operator!=(ChunkNumberValidator const& rhs) const { return not operator==(rhs); }

    /// @return 'true' if the input value is valid
    virtual bool valid(unsigned int value) const = 0;

    /// @return 'true' if the input value is 'valid' and corresponds to the 'overflow' chunk
    virtual bool overflow(unsigned int value) const;

    /// @return value corresponding to the 'overflow' chunk
    virtual unsigned int overflowValue() const;

protected:

    // No publicly available construction methods for objects of this class 

    ChunkNumberValidator();
    ChunkNumberValidator(ChunkNumberValidator const&) = default;
    ChunkNumberValidator& operator=(ChunkNumberValidator const&) = default;

private:
    
    unsigned int _id;
};

/**
 * Class ChunkNumberSingleRangeValidator extends and completes) its abstract
 * base class. The class is based on a closed interval of 'valid' chunk
 * numbers whose boundaries are passed into the normal constructor of the class.
 */
class ChunkNumberSingleRangeValidator
    :   public ChunkNumberValidator {

public:

    /**
     * Construct the validator for a specific range of chunk numbers
     *
     * @param minValue - the minimal (inclusive) number in the range
     * @param maxValue - the maximum (inclusive) number in the range
     */
    ChunkNumberSingleRangeValidator(unsigned int minValue,
                                    unsigned int maxValue);

    ChunkNumberSingleRangeValidator() = delete;

    ChunkNumberSingleRangeValidator(ChunkNumberSingleRangeValidator const&) = default;
    ChunkNumberSingleRangeValidator& operator=(ChunkNumberSingleRangeValidator const&) = default;

    ~ChunkNumberSingleRangeValidator() final = default;

    /// @return 'true' if the input value is valid
    bool valid(unsigned int value) const final;

private:

    unsigned int _minValue;
    unsigned int _maxValue;
};


/**
 * Class ChunkNumberQservValidator extends and completes) its abstract
 * base class. The class is based on the Qserv partitioning algorithm..
 */
class ChunkNumberQservValidator
    :   public ChunkNumberValidator {

public:

    /**
     * Construct the validator with a specific set of the partitioning parameters.
     * See further details on the meaning of these parameters in a documentation
     * for the constructor of class lsst::sphgeom::Chunker.
     *
     * @see lsst::sphgeom::Chunker
     */
    explicit ChunkNumberQservValidator(int32_t numStripes,
                                       int32_t numSubStripesPerStripe);

    ChunkNumberQservValidator() = delete;

    ChunkNumberQservValidator(ChunkNumberQservValidator const&) = default;
    ChunkNumberQservValidator& operator=(ChunkNumberQservValidator const&) = default;

    ~ChunkNumberQservValidator() final = default;

    /// @return 'true' if the input value is valid
    bool valid(unsigned int value) const final;

private:

    lsst::sphgeom::Chunker _chunker;
};

/**
 * Object of class ChunkNumberNotValid are thrown in the following
 * circumstances:
 *
 * - when attempting to construct an object using a non-valid (as per
 *   the validator parameter) chunk number
 *
 * - in binary operations over chunk numbers where either of the objects
 *   is not valid, or if both objects don't corresponds to the same validator.
 */
typedef std::range_error ChunkNumberNotValid;

/**
 * Class ChunkNumber is a safe abstraction for chunk numbers. Unlike
 * the basic integral type (such as 'unsigned int') this class allows
 * to restrict a range of values (chunk numbers) to some 'valid' set
 * as defined by the corresponding 'validator' object passed into
 * one of the constructors of the class.
 *
 * Objects of this class are called to be 'compatible' (hence, they can be
 * used in the corresponding binary operations) if they both are 'valid'
 * and correspond to the same instance of the 'validator'.
 *
 * Validator objects need to be passed into every explicit constructor
 * of the class.
 *
 * However, when using either the class's copy constructor or its
 * assignment operator, the 'lvalue' object will be overwritten/set
 * with all attributes of the 'rvalue' object regardless of its 'validator'
 * or other states.
 */
class ChunkNumber {

public:

    /**
     * Construct the 'overflow' chunk (as per the specified validator)
     *
     * @param pointer to a validator for a specific validation scheme
     *
     * @return valid object which also passes 'overflow()' test
     */
    static ChunkNumber makeOverflow(ChunkNumberValidator::Ptr const& validator=defaultValidator());

    /**
     * Construct an empty object which is not 'valid'.
     *
     * @param pointer to a validator for a specific validation scheme
     */
    explicit ChunkNumber(ChunkNumberValidator::Ptr const& validator=defaultValidator());

    /**
     * Attempt to construct a 'valid' object
     *
     * @param value     - the number to be associated with the chunk
     * @param validator - pointer to a validator for a specific validation scheme
     *
     * @throws ChunkNumberNotValid if the input value is not valid
     */
    explicit ChunkNumber(unsigned int value,
                         ChunkNumberValidator::Ptr const& validator=defaultValidator());

    // ATENTION: The copy constructor and the assignment operator will inherit
    // a validator of the input object.

    ChunkNumber(ChunkNumber const&) = default;
    ChunkNumber& operator=(ChunkNumber const&) = default;

    ~ChunkNumber() = default;

    /// @return pointer to a validatior
    ChunkNumberValidator::Ptr const& validator() const { return _validator; }

    /// @return 'true' if the input value is valid
    bool valid() const { return _valid; }

    /// @return 'true' if the stored value is 'valid' and corresponds to the 'overflow' chunk
    bool overflow() const { return _overflow; }

    /**
     * Explicit conversion into the numeric integer type
     *
     * @return numeric value of the chunk
     *
     * @throws ChunkNumberNotValid if the object is not valid
     */
    unsigned int value() const;

    // The binary operators will throw ChunkNumberNotValid if
    // either of the objects is not valid, or both objects aren't
    // associated to the same validator.

    bool operator==(ChunkNumber const& rhs) const;
    bool operator!=(ChunkNumber const& rhs) const { return not operator==(rhs); }
    bool operator<(ChunkNumber const& rhs) const;

    // The binary operators will construct temporary numbers from input
    // values using validators of the 'lvalue' object, and if the temporaries
    // couldn't be valid exception ChunkNumberNotValid will be thrown.

    bool operator==(unsigned int value) const;
    bool operator!=(unsigned int value) const { return not operator==(value); }
    bool operator<(unsigned int value) const;

private:

    /**
     * @return validator which is based on ChunkNumberSingleRangeValidator for
     * all range of numbers of the unsigned integer type.
     */
    static ChunkNumberValidator::Ptr const& defaultValidator();

    /**
     * Checks for validity and compatibility (both are based on the same validator)
     * of the input chunk numbers. The method is used in binary operations over
     * 
     *
     * @param lhs - the 'lvalue' object
     * @param rhs - the 'rvalue' object
     */
    static void assertBothValid(ChunkNumber const& lhs,
                                ChunkNumber const& rhs);

private:

    unsigned int _value;
    bool _valid;
    bool _overflow;

    ChunkNumberValidator::Ptr _validator;
};

/// Print the chunk number onto a stream
std::ostream& operator<<(std::ostream& os, ChunkNumber const& chunkNumber);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CHUNKNUMBER_H
