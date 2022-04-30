/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_SQL_SQLTRANSACTIONSCOPE_H
#define LSST_QSERV_SQL_SQLTRANSACTIONSCOPE_H

// System headers

// Third-party headers

// Qserv headers
#include "sql/SqlTransaction.h"
#include "util/Issue.h"

namespace lsst::qserv::sql {

/// An RAII class for handling transactions.
// All child classes should call SqlTransactionScope::create<T> to make
// new instances and have private or protected constructors. Children will
// probably need to make this class their friend.
// throwException can be overridden to throw a desired exception.
class SqlTransactionScope {
public:
    /// Always create new instances of children using this function so that verify() is called.
    /// This function allows virtual functions to be safely called to check on the object
    /// immediately after the object has been created.
    template <class T>
    static std::shared_ptr<T> create(sql::SqlConnection& conn) {
        std::shared_ptr<T> transScope(new T(conn));
        transScope->verify();
        return transScope;
    }

    /// Calls throwException if errObj is set.
    virtual void verify();

    // Instances cannot be copied
    SqlTransactionScope(SqlTransactionScope const&) = delete;
    SqlTransactionScope& operator=(SqlTransactionScope const&) = delete;

    /// Destructor aborts transaction if it was not explicitly committed
    /// or aborted. If error happens then no exception is generated
    /// (destructors cannot throw).
    virtual ~SqlTransactionScope();

    /// Override to throw an appropriate exception.
    virtual void throwException(util::Issue::Context const& ctx, std::string const& msg);

    /// Explicitly commit transaction, throws via throwException().
    virtual void commit();

    /// Explicitly abort transaction, throws via throwException().
    virtual void abort();

    /// Query to find out if this represents an active transaction
    virtual bool isActive() const { return trans.isActive(); }

protected:
    /// Constructor takes connection instance. It starts transaction.
    SqlTransactionScope(sql::SqlConnection& conn) : errObj(), trans(conn, errObj) {}

    sql::SqlErrorObject errObj;  // this must be declared before _trans
    sql::SqlTransaction trans;
};

}  // namespace lsst::qserv::sql

#endif /* LSST_QSERV_SQL_SQLTRANSACTIONSCOPE_H */
