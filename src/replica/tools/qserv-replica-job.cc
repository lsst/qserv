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

// System headers
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/AbortTransactionApp.h"
#include "replica/AdminApp.h"
#include "replica/ApplicationColl.h"
#include "replica/ChunksApp.h"
#include "replica/DeleteWorkerApp.h"
#include "replica/FixUpApp.h"
#include "replica/ClusterHealthApp.h"
#include "replica/DirectorIndexApp.h"
#include "replica/MoveApp.h"
#include "replica/PurgeApp.h"
#include "replica/RebalanceApp.h"
#include "replica/ReplicateApp.h"
#include "replica/SqlApp.h"
#include "replica/SyncApp.h"
#include "replica/VerifyApp.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

ApplicationColl getAppColl() {
    ApplicationColl coll;
    coll.add<AbortTransactionApp>("ABORT-TRANS");
    coll.add<AdminApp>("ADMIN");
    coll.add<ChunksApp>("CHUNKS");
    coll.add<DeleteWorkerApp>("DELETE-WORKER");
    coll.add<FixUpApp>("FIXUP");
    coll.add<ClusterHealthApp>("CLUSTER-HEALTH");
    coll.add<DirectorIndexApp>("INDEX");
    coll.add<MoveApp>("MOVE");
    coll.add<PurgeApp>("PURGE");
    coll.add<RebalanceApp>("REBALANCE");
    coll.add<ReplicateApp>("REPLICATE");
    coll.add<SqlApp>("SQL");
    coll.add<SyncApp>("SYNC");
    coll.add<VerifyApp>("VERIFY");
    return coll;
}
}  // namespace

int main(int argc, char* argv[]) {
    try {
        return getAppColl().run(argc, argv);
    } catch (exception const& ex) {
        cerr << argv[0] << ": the application '" << argv[1] << "' failed, exception: " << ex.what() << endl;
        return 1;
    }
}
