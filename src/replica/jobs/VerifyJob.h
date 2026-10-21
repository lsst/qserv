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
#ifndef LSST_QSERV_REPLICA_VERIFYJOB_H
#define LSST_QSERV_REPLICA_VERIFYJOB_H

// System headers
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers
#include "replica/jobs/Job.h"
#include "replica/util/ReplicaInfo.h"

// Forward declarations
namespace lsst::qserv::replica {
class Controller;
class FindRequest;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ReplicaDiff represents a difference between two replica information
 * objects which are stored here.
 *
 * NOTE: a reason why a special class (versus an overloaded version of
 * operator==) is needed to differentiate between replicas is due to
 * greater flexibility of the special class which not only encapsulates both
 * replicas within a single object, but it also allows compare objects
 * in a specific context of the replica verification job. Specific aspects
 * of the replica differences could be also reported.
 */
class ReplicaDiff {
public:
    /**
     * Default constructor create an object which exhibits "no difference"
     * behavior.
     */
    ReplicaDiff();

    /**
     * The normal constructor
     *
     * @param replica1 a reference to the the 'older' replica object
     * @param replica2 a reference to the the 'newer' replica object
     */
    ReplicaDiff(ReplicaInfo const& replica1, ReplicaInfo const& replica2);

    ReplicaDiff(ReplicaDiff const&) = default;
    ReplicaDiff& operator=(ReplicaDiff const&) = default;

    ~ReplicaDiff() = default;

    /// @return a reference to the the 'older' replica object
    ReplicaInfo const& replica1() const { return _replica1; }

    /// @return a reference to the the 'newer' replica object
    ReplicaInfo const& replica2() const { return _replica2; }

    /**
     * @return 'true' if the object encapsulates two snapshots referring to the same replica.
     */
    bool isSelf() const;

    /**
     * @return 'true' in case if there are differences between replicas. Specific aspects
     *  of the difference can be explored by directly comparing the replica objects.
     */
    bool operator()() const { return _notEqual; }

    // Specific tests

    bool statusMismatch() const { return _statusMismatch; }
    bool numFilesMismatch() const { return _numFilesMismatch; }
    bool fileNamesMismatch() const { return _fileNamesMismatch; }
    bool fileSizeMismatch() const { return _fileSizeMismatch; }
    bool fileCsMismatch() const { return _fileCsMismatch; }
    bool fileMtimeMismatch() const { return _fileMtimeMismatch; }

    /// @return a compact string representation of the failed tests
    std::string const& flags2string() const;

private:
    ReplicaInfo _replica1;  ///< older replia
    ReplicaInfo _replica2;  ///< newer replica

    bool _notEqual;
    bool _statusMismatch;
    bool _numFilesMismatch;
    bool _fileNamesMismatch;
    bool _fileSizeMismatch;
    bool _fileCsMismatch;
    bool _fileMtimeMismatch;

    mutable std::string _flags;  ///< computed and cached first time requested
};

std::ostream& operator<<(std::ostream& os, ReplicaDiff const& ri);

/**
 * Class VerifyJob represents a tool which will find go over all replicas
 * of all chunks and databases on all worker nodes, check if replicas still
 * exist, then verify a status of each replica. The new status will be compared
 * against the one which exists in the database. This will include:
 *   - file sizes
 *   - modification timestamps of files
 *   - control/check sums of files belonging to the replicas
 *
 * Any differences will get reported to a subscriber via a specific callback
 * function. The new status of a replica will be also recorded within the database.
 *
 * @note Notifications are sent using a pool of the Boost ASIO threads of the Controller.
 */
class VerifyJob : public Job {
public:
    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<VerifyJob>)> CallbackType;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(std::shared_ptr<VerifyJob>, ReplicaDiff const&,
                               std::vector<ReplicaDiff> const&)>
            CallbackTypeOnDiff;

    /// @return the unique name distinguishing this class from other types of jobs
    static std::string typeName();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param maxReplicas maximum number of replicas to process simultaneously
     *  (must be greater than 0).
     * @param computeCheckSum compute check/control sum on each file if set to 'true'
     * @param onReplicaDifference callback function to be called when two replicas won't match
     * @param controller for launching requests
     * @param parentJobId an identifier of a parent job
     * @param onFinish a callback function to be called upon a completion of the job
     * @param priority the priority level of the job
     * @return pointer to the created object
     * @throws std::invalid_argument if maxReplicas is 0
     */
    static std::shared_ptr<VerifyJob> create(std::size_t maxReplicas, bool computeCheckSum,
                                             CallbackTypeOnDiff const& onReplicaDifference,
                                             std::shared_ptr<Controller> const& controller,
                                             std::string const& parentJobId, CallbackType const& onFinish,
                                             int priority);

    VerifyJob() = delete;
    VerifyJob(VerifyJob const&) = delete;
    VerifyJob& operator=(VerifyJob const&) = delete;

    ~VerifyJob() final = default;

    /// @return maximum number of replicas to be allowed processed simultaneously
    std::size_t maxReplicas() const { return _maxReplicas; }

    /// @return true if file check/control sums need to be recomputed
    bool computeCheckSum() const { return _computeCheckSum; }

    /// @see Job::extendedPersistentState()
    std::list<std::pair<std::string, std::string>> extendedPersistentState() const override;

protected:
    virtual void startImpl(replica::Lock const& lock) final;
    virtual void cancelImpl(replica::Lock const& lock) final;
    virtual void notify(replica::Lock const& lock) final;

private:
    VerifyJob(std::size_t maxReplicas, bool computeCheckSum, CallbackTypeOnDiff const& onReplicaDifference,
              std::shared_ptr<Controller> const& controller, std::string const& parentJobId,
              CallbackType const& onFinish, int priority);

    /**
     * The callback function to be invoked on a completion of each request.
     * @param request a pointer to a request
     */
    void _onRequestFinish(std::shared_ptr<FindRequest> const& request);

    /**
     * Find the next replicas to be inspected.
     *
     * @param lock the lock on Job::_mtx must be acquired by a caller of the method
     * @param replicas a collection of replicas returned from the database
     * @param numReplicas a desired number of replicas to be pulled from the database for processing.
     */
    void _nextReplicas(replica::Lock const& lock, std::vector<ReplicaInfo>& replicas,
                       std::size_t numReplicas);

    // Input parameters

    std::size_t const _maxReplicas;
    bool const _computeCheckSum;

    /// Client-defined function to be called upon the completion of the job
    /// @note is reset when the job finishes
    CallbackType _onFinish;

    /// Client-defined function to be called when two replicas won't match
    CallbackTypeOnDiff _onReplicaDifference;

    /// The current (last) batch if replicas which are being inspected.
    /// The replicas are registered by the corresponding request IDs.
    std::map<std::string, ReplicaInfo> _replicas;

    /// The current (last) batch of requests registered by their IDs
    std::map<std::string, std::shared_ptr<FindRequest>> _requests;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_VERIFYJOB_H
