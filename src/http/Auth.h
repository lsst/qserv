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
#ifndef LSST_QSERV_HTTP_AUTH_H
#define LSST_QSERV_HTTP_AUTH_H

// System headers
#include <stdexcept>
#include <string>

// This header declarations
namespace lsst::qserv::http {

/// The enumeration type which is used for configuring/enforcing
/// module's authorization requirements.
enum class AuthType { BASIC, REQUIRED, NONE };

/// Class AuthError represent exceptions thrown when the authorization
/// requirements aren't met.
class AuthError : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

/// Class AuthContext is used to pass the authorization keys and login credential
/// to modules. The context is used only by modules where AuthTime::REQUIRED was requested
/// by a configuration of the service. Users can be authorized in two ways:
/// - by providing the basic authentication credentials (user and password) in the request's header
/// - by providing an authorization key (authKey or adminAuthKey) in the body of a request
///
/// If both were provided then the basic authentication credentials take precedence.
class AuthContext {
public:
    AuthContext() = default;
    AuthContext(std::string const& user_, std::string const& password_, std::string const& authKey_,
                std::string const& adminAuthKey_)
            : user(user_), password(password_), authKey(authKey_), adminAuthKey(adminAuthKey_) {}
    AuthContext(AuthContext const&) = default;
    AuthContext& operator=(AuthContext const&) = default;

    std::string user;          ///< The name of the user (if any)
    std::string password;      ///< The password of the user (if any)
    std::string authKey;       ///< The authorization key for normal operations
    std::string adminAuthKey;  ///< The administrator-level authorization key
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_AUTH_H
