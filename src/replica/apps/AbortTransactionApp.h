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
#ifndef LSST_QSERV_REPLICA_ABORTTRANSACTIONAPP_H
#define LSST_QSERV_REPLICA_ABORTTRANSACTIONAPP_H

// System headers
#include <cstdint>

// Qserv headers
#include "replica/apps/Application.h"
#include "replica/util/Common.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class AbortTransactionApp implements a tool which aborts a transaction by dropping
 * MySQL table partitions corresponding to the transaction at the relevant worker
 * databases. And while doing so, the application will make the best effort to leave
 * worker nodes as balanced as possible.
 */
class AbortTransactionApp : public Application {
public:
    typedef std::shared_ptr<AbortTransactionApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     * @param argc the number of command-line arguments
     * @param argv the vector of command-line arguments
     */
    static Ptr create(int argc, char* argv[]);

    AbortTransactionApp() = delete;
    AbortTransactionApp(AbortTransactionApp const&) = delete;
    AbortTransactionApp& operator=(AbortTransactionApp const&) = delete;

    ~AbortTransactionApp() override = default;

protected:
    int runImpl() final;

private:
    AbortTransactionApp(int argc, char* argv[]);

    TransactionId _transactionId = 0;
    bool _allWorkers = false;
    unsigned int _reportLevel = 0;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_ABORTTRANSACTIONAPP_H */
