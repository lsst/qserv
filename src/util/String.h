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
     * @param skipEmpty The optional flag that if 'true' would eliminate empty strings from
     *   the result. Otherwise the empty strings found between the delimiter will
     *   be preserved in the result.
     * @note The result filtering requested by a value of the parameter 'skipEmpty'
     *   also applies to a secenario when the input string is empty. In particular,
     *   in the 'skipEmpty' mode the output collection will be empty. Otherwise, the collection
     *   will have exactly one element - the empty string.
     * @return A collection of strings resulting from splitting the input string into
     *   sub-strings using the delimiter. The delimiter won't be included into the substrings.
     */
    static std::vector<std::string> split(std::string const& str, std::string const& delimiter,
                                          bool skipEmpty = false);

    /**
     * Parse the input string into a collection of numbers (int).
     * @param str The input string to be parsed.
     * @param delimiter A delimiter.
     * @param throwOnError The optional flag telling the function what to do when conversions
     *   from substrings to numbers fail. If the flag is set to 'true' then an exception
     *   will be thrown. Otherwise, the default value will be placed into the output collection.
     * @param defaultVal The optional default value to be injected where applies.
     * @param skipEmpty The optional flag that if 'true' would eliminate empty substrings from
     *   parsing into the the numeric result. Otherwise the behavior of the methjod will be driven
     *   by a value of the parameter 'throwOnError'.
     * @return A collection of numbers found in the input string.
     * @throw std::invalid_argument If substrings found within the input string can't be
     *   interpreted as numbers of the given type.
     * @see function split
     */
    static std::vector<int> parseToVectInt(std::string const& str, std::string const& delimiter,
                                           bool throwOnError = true, int defaultVal = 0,
                                           bool skipEmpty = false);

    /**
     * Parse the input string into a collection of numbers (std::uint64_t).
     * @see function parseToVectInt
     */
    static std::vector<std::uint64_t> parseToVectUInt64(std::string const& str, std::string const& delimiter,
                                                        bool throwOnError = true,
                                                        std::uint64_t defaultVal = 0ULL,
                                                        bool skipEmpty = false);

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

    /**
     * Encode the input sequence of bytes into the hexadecimal representation packaged
     * into a string.
     *
     * For example, the method will convert a sequence of bytes as shown below:
     * @code
     *   // (prefix="", lowerCase=false)
     *   {10,17,255,210} -> "0A11FFD2"
     *
     *   // (prefix="0x", lowerCase=false)
     *   {10,17,255,210} -> "0x0A11FFD2"
     *
     *   // (prefix="", lowerCase=true)
     *   {10,17,255,210} -> "0a11ffd2"
     * @endcode
     * @param ptr A pointer to the byte sequence.
     * @param length The number of bytes to translate.
     * @param prefix The optional prefix for non-empty input.
     * @param lowerCase The optional flag indicating of the the lower case version
     *   of the hexadecimal output is required.
     * @return The encoded sequence of bytes or the empty string if the length=0.
     * @throw std::invalid_argument If the pointer is nullptr.
     */
    static std::string toHex(char const* ptr, std::size_t length, std::string const& prefix = std::string(),
                             bool lowerCase = false);

    /**
     * Decode the hexadecimal string that may have an optional prefix into a string.
     *
     * For example, the method will convert the  of bytes as shown below:
     * @code
     *   // (prefix="", upper case input)
     *   "0A11FFD2" -> {10,17,255,210}
     *
     *   // (prefix="0x", upper case input)
     *   "0x0A11FFD2" -> {10,17,255,210}
     *
     *   // (prefix="", lower case input)
     *   "0a11ffd2" -> {10,17,255,210}
     * @endcode
     *
     * @note The translator accepts mixed-case case characters in the input string.
     * @param hex The string to be decoded.
     * @param prefix The optional prefix to be ignored from the input.
     * @return The decoded sequence of bytes or the empty string if the input has no significant
     *   characters beyond the optonal prefix.
     * @throw std::invalid_argument If the input doesn't start with the specified (if any)
     *    prefix, or for an odd number of the significant (after the optional prefix) characters
     *   in the input.
     * @throw std::range_error For non-hexadecimal characters in the input.
     */
    static std::string fromHex(std::string const& hex, std::string const& prefix = std::string());

    /// @param str A string to be translated
    /// @return The string with all characters converted to lower case.
    static std::string toLower(std::string const& str);

    /// @param str A string to be translated
    /// @return The string with all characters converted to upper case.
    static std::string toUpper(std::string const& str);

    /**
     * Encode the input sequence of bytes into the Base64 representation packaged
     * into a string with ('=') padding as needed.
     *
     * For example, the method will convert a sequence of characters as shown below:
     * @code
     *   "0123456789" -> "MDEyMzQ1Njc4OQ=="
     * @endcode
     * @param ptr A pointer to the byte sequence.
     * @param length The number of bytes to translate.
     * @return The encoded sequence of bytes or the empty string if the length=0.
     * @throw std::invalid_argument If the pointer is nullptr.
     */
    static std::string toBase64(char const* ptr, std::size_t length);
    static std::string toBase64(std::string const& str) { return toBase64(str.data(), str.size()); }

    /**
     * Decode the Base64-encoded (padded with '=' as needed) string into the binary string.
     *
     * For example, the method will decode the encoded Base64 string as shown below:
     * @code
     *   "MDEyMzQ1Njc4OQ==" -> "0123456789"
     * @endcode
     *
     * @param str The string to be decoded.
     * @return The decoded sequence of bytes or the empty string if the input is emoty.
     * @throw std::range_error For non-base64 characters in the input.
     */
    static std::string fromBase64(std::string const& str);

    /**
     * Generate a unique name based on the input model. The model is expected
     * to contain '%' characters which will be replaced with random digits to make
     * the file name unique.
     * @param model - the model for the name generation
     * @return a unique name generated from the input model
     */
    static std::string translateModel(std::string const& model);
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_STRING_H
