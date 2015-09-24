/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 /**
  * @file
  *
  * @brief Provide generic output operator for iterable data structures
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */


#ifndef LSST_QSERV_UTIL_ITERABLERFORMATTER_H
#define LSST_QSERV_UTIL_ITERABLERFORMATTER_H

// System headers
#include <iostream>
#include <iterator>
#include <utility>

// Third-party headers

// LSST headers

// Qserv headers

namespace lsst {
namespace qserv {
namespace util {

/**
 *  Provide generic output operator for iterable data structures
 *
 *  Output format is [a, b, c, ...]
 *  The elements stored in the iterable data structure
 *  must provide an ouput operator.
 *  Examples:
 *  - works fine for std::vector<std::string>,
 *  - doesn't work for std::multimap.
 */
template <typename Iter>
class IterableFormatter
{
public:

    /**
     *  Create a printable wrapper for an iterable data structure
     *
     *  @param x:       an iterable data structure
     *  @param first:   start printing from x[first]
     *  @param open:    opening bracket, NULL is undefined behaviour
     *  @param close:   closing bracket, NULL is undefined behaviour
     *  @param sep:     separator between elements, NULL is undefined behaviour
     */
    explicit IterableFormatter(Iter begin, Iter end,
                               char const *const open, char const *const close,
                               char const *const sep) :
            _begin(begin), _end(end), _open(open), _close(close), _sep(sep) {
    }

    /**
     *  Output operator for IterableFormatter<Iterable>
     *
     *  @param out
     *  @param iterableFormatter
     *  @return an output stream, with no newline at the end
     */
    friend std::ostream& operator<<(std::ostream& os,
                                    IterableFormatter<Iter> const& self) {
        typedef _item_fmt<typename std::iterator_traits<Iter>::value_type> item_fmt;
        os << self._open;
        auto it = self._begin;
        if (it != self._end) {
            item_fmt::fmt(os, *it);
            ++it;
        }
        for ( ; it != self._end; ++it) {
            os << self._sep;
            item_fmt::fmt(os, *it);
        }
        os << self._close;
        return os;
    }

private:
    Iter _begin;
    Iter _end;
    char const *const _open;
    char const *const _close;
    char const *const _sep;

    // generic item formatting
    template <typename T>
    struct _item_fmt {
        static void fmt(std::ostream& os, const T& item) { os << item; }
    };
    // specialization for pair
    template <typename U, typename V>
    struct _item_fmt<std::pair<U, V>> {
        static void fmt(std::ostream& os, const std::pair<U, V>& item) {
            os << '(' << item.first << ", " << item.second << ')'; }
    };

};

template <typename Iterator>
IterableFormatter<Iterator> printable(Iterator begin, Iterator end,
                                  char const *const open = "[", char const *const close = "]",
                                  char const *const sep = ", ")
{
    return IterableFormatter<Iterator>(begin, end, open, close, sep);
}


/**
 *  Create a printable wrapper for an iterable data structure
 *
 *  @param x:       an iterable data structure
 *  @param first:   start printing from x[first]
 *  @param open:    opening bracket, NULL is undefined behaviour
 *  @param close:   closing bracket, NULL is undefined behaviour
 *  @param sep:     separator between elements, NULL is undefined behaviour
 *  @return:        an object wrapping x and providing an output operator
 */
template <typename Iterable>
IterableFormatter<typename Iterable::const_iterator> printable(Iterable const& x,
                                  char const *const open = "[", char const *const close = "]",
                                  char const *const sep = ", ")
{
    return printable(x.begin(), x.end(), open, close, sep);
}

}}} // namespace lsst::qserv::util
#endif // LSST_QSERV_UTIL_ITERABLERFORMATTER_H
