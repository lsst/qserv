// -*- LSST-C++ -*-
/*
 * LSST Data Management System
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

#ifndef LSST_QSERV_UTIL_STRING_H
#define LSST_QSERV_UTIL_STRING_H

// System headers
#include <sstream>
#include <string>
#include <string>
#include <vector>

namespace lsst::qserv::util {

/// Functions to help with string processing.
class String {
public:
    /**
     * Split the input string into substrings using the specified delimiter.
     * @param str The input string to be parsed.
     * @param delimiter A delimiter.
     * @param greedy The optional flag that if 'true' would eliminate empty strings from
     *   the result. Otherwise the empty strings found between the delimiter will
     *   be preserved in the result.
     * @note The result filtering requested by a value of the parameter 'greedy'
     *   also applies to a secenario when the input string is empty. In particular,
     *   in the 'greedy' mode the output collection will be empty. Otherwise, the collection
     *   will have exactly one element - the empty string.
     * @return A collection of strings resulting from splitting the input string into
     *   sub-strings using the delimiter. The delimiter won't be included into the substrings.
     */
    static std::vector<std::string> split(std::string const& str, std::string const& delimiter,
                                          bool greedy = false);

    /**
     * Parse the input string into a collection of numbers (int).
     * @param str The input string to be parsed.
     * @param delimiter A delimiter.
     * @param throwOnError The optional flag telling the function what to do when conversions
     *   from substrings to numbers fail. If the flag is set to 'true' then an exception
     *   will be thrown. Otherwise, the default value will be placed into the output collection.
     * @param defaultVal The optional default value to be injected where applies.
     * @param greedy The optional flag that if 'true' would eliminate empty substrings from
     *   parsing into the the numeric result. Otherwise the behavior of the methjod will be driven
     *   by a value of the parameter 'throwOnError'.
     * @return A collection of numbers found in the input string.
     * @throw std::invalid_argument If substrings found within the input string can't be
     *   interpreted as numbers of the given type.
     * @see function split
     */
    static std::vector<int> parseToVectInt(std::string const& str, std::string const& delimiter,
                                           bool throwOnError = true, int defaultVal = 0, bool greedy = false);

    /**
     * Parse the input string into a collection of numbers (std::uint64_t).
     * @see function parseToVectInt
     */
    static std::vector<std::uint64_t> parseToVectUInt64(std::string const& str, std::string const& delimiter,
                                                        bool throwOnError = true,
                                                        std::uint64_t defaultVal = 0ULL, bool greedy = false);

    /**
     * Pack an iterable collection into a string.
     * @param coll The input collection.
     * @param delimiter An (optional) delimiter between elements.
     * @param openingBracket An (optional) opening bracket.
     * @param closingBracket An (optional) closing bracket.
     * @return The string representation of the collection.
     */
    template <typename COLLECTION>
    static std::string toString(COLLECTION const& coll, std::string const& delimiter = ",",
                                std::string const& openingBracket = "",
                                std::string const& closingBracket = "") {
        std::ostringstream ss;
        for (auto itr = coll.begin(); itr != coll.end(); ++itr) {
            if (coll.begin() != itr) ss << delimiter;
            ss << openingBracket << *itr << closingBracket;
        }
        return ss.str();
    }
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_STRING_H