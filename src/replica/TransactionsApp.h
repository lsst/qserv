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
#ifndef LSST_QSERV_REPLICA_TRANSACTIONAPP_H
#define LSST_QSERV_REPLICA_TRANSACTIONAPP_H

// System headers
#include <cstdint>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Application.h"
#include "replica/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class TransactionInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class TransactionsApp implements a tool for testing super-transactions.
 */
class TransactionsApp : public Application {
public:
    typedef std::shared_ptr<TransactionsApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc the number of command-line arguments
     * @param argv  the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    TransactionsApp() = delete;
    TransactionsApp(TransactionsApp const&) = delete;
    TransactionsApp& operator=(TransactionsApp const&) = delete;

    ~TransactionsApp() override = default;

protected:
    int runImpl() final;

private:
    TransactionsApp(int argc, char* argv[]);

    void _print(TransactionInfo const& info) const;
    void _print(std::vector<TransactionInfo> const& collection) const;

    std::string _operation;     ///< An operation over transactions
    std::string _databaseName;  ///< The name of a database associated with a transaction(s))

    TransactionId _id = 0;  ///< A unique identifier of an existing transaction
    std::string _state;     ///< The new state of an existing transaction.

    size_t _sqlPageSize = 20;  ///< The number of rows per page printed in a table of
                               ///  transactions (0 means no pages)
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_TRANSACTIONAPP_H */
