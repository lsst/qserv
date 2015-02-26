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
/// \brief This file contains the NormalizedAngleInterval class implementation.

#include "NormalizedAngleInterval.h"

#include <ostream>
#include <stdexcept>

#include "SpatialRelation.h"


namespace lsst {
namespace sg {

NormalizedAngleInterval::NormalizedAngleInterval(Angle x, Angle y) {
    if (x.isNan() || y.isNan()) {
        *this = empty();
        return;
    }
    if (!x.isNormalized() || !y.isNormalized()) {
        if (x > y) {
            throw std::invalid_argument(
                "invalid NormalizedAngleInterval endpoints");
        }
        if (y - x >= Angle(2.0 * PI)) {
            *this = full();
            return;
        }
    }
    _a = NormalizedAngle(x);
    _b = NormalizedAngle(y);
}

bool NormalizedAngleInterval::contains(
    NormalizedAngleInterval const & x) const
{
    if (x.isEmpty()) {
        return true;
    }
    if (isEmpty()) {
        return false;
    }
    if (x.wraps()) {
        if (!wraps()) {
            return isFull();
        }
    } else if (wraps()) {
        return x._a >= _a || x._b <= _b;
    }
    return x._a >= _a && x._b <= _b;
}

bool NormalizedAngleInterval::isDisjointFrom(
    NormalizedAngleInterval const & x) const
{
    if (x.isEmpty() || isEmpty()) {
        return true;
    }
    if (x.wraps()) {
        return wraps() ? false : (x._a > _b && x._b < _a);
    }
    if (wraps()) {
        return _a > x._b && _b < x._a;
    }
    return x._b < _a || x._a > _b;
}

int NormalizedAngleInterval::relate(NormalizedAngle x) const {
    if (isEmpty()) {
        if (x.isNan()) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    }
    if (x.isNan()) {
        return CONTAINS | DISJOINT;
    }
    if (_a == x && _b == x) {
        return CONTAINS | INTERSECTS | WITHIN;
    }
    if (intersects(x)) {
        return CONTAINS | INTERSECTS;
    }
    return DISJOINT;
}

int NormalizedAngleInterval::relate(NormalizedAngleInterval const & x) const {
    if (isEmpty()) {
        if (x.isEmpty()) {
            return CONTAINS | DISJOINT | WITHIN;
        }
        return DISJOINT | WITHIN;
    }
    if (x.isEmpty()) {
        return CONTAINS | DISJOINT;
    }
    if (_a == x._a && _b == x._b) {
        return CONTAINS | INTERSECTS | WITHIN;
    }
    // The intervals are not the same, and neither is empty.
    if (wraps()) {
        if (x.wraps()) {
            // Both intervals wrap.
            if (_a <= x._a && _b >= x._b) {
                return CONTAINS | INTERSECTS;
            }
            if (_a >= x._a && _b <= x._b) {
                return INTERSECTS | WITHIN;
            }
            return INTERSECTS;
        }
        // x does not wrap.
        if (x.isFull()) {
            return INTERSECTS | WITHIN;
        }
        if (_a <= x._a || _b >= x._b) {
            return CONTAINS | INTERSECTS;
        }
        return (_a > x._b && _b < x._a) ? DISJOINT : INTERSECTS;
    }
    if (x.wraps()) {
        // This interval does not wrap.
        if (isFull()) {
            return CONTAINS | INTERSECTS;
        }
        if (x._a <= _a || x._b >= _b) {
            return INTERSECTS | WITHIN;
        }
        return (x._a > _b && x._b < _a) ? DISJOINT : INTERSECTS;
    }
    // Neither interval wraps.
    if (_a <= x._a && _b >= x._b) {
        return CONTAINS | INTERSECTS;
    }
    if (_a >= x._a && _b <= x._b) {
        return INTERSECTS | WITHIN;
    }
    return (_a <= x._b && _b >= x._a) ? INTERSECTS : DISJOINT;
}

NormalizedAngleInterval & NormalizedAngleInterval::clipTo(
    NormalizedAngleInterval const & x)
{
    if (x.isEmpty()) {
        *this = empty();
    } else if (contains(x._a)) {
        if (contains(x._b)) {
            // Both endpoints of x are in this interval. This interval
            // either contains x, in which case x is the exact intersection,
            // or the intersection consists of [_a,x._b] ⋃ [x._a,_b].
            // In both cases, the envelope of the intersection is the shorter
            // of the two intervals.
            if (getSize() >= x.getSize()) {
                *this = x;
            }
        } else {
            _a = x._a;
        }
    } else if (contains(x._b)) {
        _b = x._b;
    } else if (x.isDisjointFrom(_a)) {
        *this = empty();
    }
    return *this;
}

NormalizedAngleInterval & NormalizedAngleInterval::expandTo(
    NormalizedAngle x)
{
    if (isEmpty()) {
        *this = NormalizedAngleInterval(x);
    } else if (!contains(x)) {
        if (x.getAngleTo(_a) > _b.getAngleTo(x)) {
            _b = x;
        } else {
            _a = x;
        }
    }
    return *this;
}

NormalizedAngleInterval & NormalizedAngleInterval::expandTo(
    NormalizedAngleInterval const & x)
{
    if (!x.isEmpty()) {
        if (contains(x._a)) {
            if (contains(x._b)) {
                // Both endpoints of x are in this interval. This interval
                // either contains x, in which case this interval is the
                // desired union, or the union is the full interval.
                if (wraps() != x.wraps()) {
                    *this = full();
                }
            } else {
                _b = x._b;
            }
        } else if (contains(x._b)) {
            _a = x._a;
        } else if (isEmpty() || x.contains(_a)) {
            *this = x;
        } else if (_b.getAngleTo(x._a) < x._b.getAngleTo(_a)) {
            _b = x._b;
        } else {
            _a = x._a;
        }
    }
    return *this;
}

NormalizedAngleInterval NormalizedAngleInterval::dilatedBy(Angle x) const {
    if (isEmpty() || isFull() || x == Angle(0.0) || x.isNan()) {
        return *this;
    }
    Angle a = _a - x;
    Angle b = _b + x;
    if (x > Angle(0.0)) {
        // x is a dilation.
        if (x >= Angle(PI)) { return full(); }
        if (wraps()) {
            // The undilated interval wraps. If the dilated one does not,
            // then decreasing a from _a and increasing b from _b has
            // caused b and a to cross.
            if (a <= b) { return full(); }
        } else {
            // The undilated interval does not wrap. If either a or b
            // is not normalized then the dilated interval must either
            // wrap or be full.
            if (a < Angle(0.0)) {
                a = a + Angle(2.0 * PI);
                if (a <= b) { return full(); }
            }
            if (b > Angle(2.0 * PI)) {
                b = b - Angle(2.0 * PI);
                if (a <= b) { return full(); }
            }
        }
    } else {
        // x is an erosion.
        if (x <= Angle(-PI)) { return empty(); }
        if (wraps()) {
            // The uneroded interval wraps. If either a or b is not
            // normalized, then either the eroded interval does not wrap,
            // or it is empty.
            if (a > Angle(2.0 * PI)) {
                a = a - Angle(2.0 * PI);
                if (a > b) { return empty(); }
            }
            if (b < Angle(0.0)) {
                b = b + Angle(2.0 * PI);
                if (a > b) { return empty(); }
            }
        } else {
            // The uneroded interval does not wrap. If the eroded one does,
            // then increasing a from _a and decreasing b from _b has
            // caused a and b to cross.
            if (a > b) { return empty(); }
        }
    }
    return NormalizedAngleInterval(a, b);
}

std::ostream & operator<<(std::ostream & os,
                          NormalizedAngleInterval const & i)
{
    return os << '[' << i.getA() << ", " << i.getB() << ']';
}

}} // namespace lsst::sg
