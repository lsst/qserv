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

#ifndef LSST_SG_ANGLEINTERVAL_H_
#define LSST_SG_ANGLEINTERVAL_H_

/// \file
/// \brief This file defines a class for representing angle intervals.

#include "Angle.h"
#include "Interval.h"


namespace lsst {
namespace sg {

/// `AngleInterval` represents closed intervals of arbitrary angles.
class AngleInterval : public Interval<AngleInterval, Angle> {
    typedef Interval<AngleInterval, Angle> Base;
public:
    // Factory functions
    static AngleInterval fromDegrees(double x, double y) {
        return AngleInterval(Angle::fromDegrees(x),
                             Angle::fromDegrees(y));
    }

    static AngleInterval fromRadians(double x, double y) {
        return AngleInterval(Angle::fromRadians(x),
                             Angle::fromRadians(y));
    }

    static AngleInterval empty() {
        return AngleInterval();
    }

    static AngleInterval full() {
        return AngleInterval(Angle(-std::numeric_limits<double>::infinity()),
                             Angle(std::numeric_limits<double>::infinity()));
    }

    // Delegation to base class constructors
    AngleInterval() : Base() {}

    explicit AngleInterval(Angle x) : Base(x) {}

    AngleInterval(Angle x, Angle y) : Base(x, y) {}

    AngleInterval(Base const & base) : Base(base) {}
};

}} // namespace lsst::sg

#endif // LSST_SG_ANGLEINTERVAL_H_
