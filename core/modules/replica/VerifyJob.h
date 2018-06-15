/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// VerifyJob.h declares:
///
/// class  VerifyJob
///
/// (see individual class documentation for more information)

// System headers
#include <functional>
#include <string>

// Qserv headers
#include "replica/Job.h"
#include "replica/FindRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class ReplicaDiff represents a difference between two replica information
 * objects which are stored here.
 *
 * NOTE: a reason why a special class (versus an overloaded version of
 * operator==) is needed to differrenciate between replicas is due to
 * greater flexibily of the special class which not only encapsulates both
 * replicas within a single object, but it also allows compare ovejcts
 * in a specific context of the replica verification job. Specific aspects
 * of the replica diffences could be also reported.
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
     * @param replica1 - a reference to the the 'older' replica object
     * @param replica2 - a reference to the the 'newer' replica object
     */
    ReplicaDiff(ReplicaInfo const& replica1,
                ReplicaInfo const& replica2);

    ReplicaDiff(ReplicaDiff const&) = default;
    ReplicaDiff& operator=(ReplicaDiff const&) = default;

    ~ReplicaDiff() = default;

    /// @return a reference to the the 'older' replica object
    ReplicaInfo const& replica1() const { return _replica1; }

    /// @return a reference to the the 'newer' replica object
    ReplicaInfo const& replica2() const { return _replica2; }

    /**
     * @return 'true' if the object encapculates two snapshots refferring
     * to the same replica.
     */
    bool isSelf() const;

    /**
     * The comporision operator
     *
     * @return 'true' in case if there are diffences between replicas. Specific aspects
     * of the difference can be explored by directly comparing the replica objects.
     */
    bool operator()() const { return _notEqual; }

    // Specific tests

    bool statusMismatch()    const { return _statusMismatch; }
    bool numFilesMismatch()  const { return _numFilesMismatch; }
    bool fileNamesMismatch() const { return _fileNamesMismatch; }
    bool fileSizeMismatch()  const { return _fileSizeMismatch; }
    bool fileCsMismatch()    const { return _fileCsMismatch; }
    bool fileMtimeMismatch() const { return _fileMtimeMismatch; }

    /// @return a compact string representation of the failed tests
    std::string const& flags2string() const;

private:

    ReplicaInfo _replica1; ///< older replia
    ReplicaInfo _replica2; ///< newer replica

    bool _notEqual;
    bool _statusMismatch;
    bool _numFilesMismatch;
    bool _fileNamesMismatch;
    bool _fileSizeMismatch;
    bool _fileCsMismatch;
    bool _fileMtimeMismatch;

    mutable std::string _flags;     ///< computed and cached first time requested
};

/// Overloaded streaming operator for type ReplicaDiff
std::ostream& operator<<(std::ostream& os, ReplicaDiff const& ri);

/**
  * Class VerifyJob represents a tool which will find go over all replicas
  * of all chunks and databases on all worker nodes, check if replicas still
  * exist, then verify a status of each replica. The new status will be compared
  * against the one which exists in the database. This will include:
  *   - file sizes
  *   - modification timestamps of files
  *   - control/check sums of files constituiting the replicas
  *
  * Any differences will get reported to a subscriber via a specific callback
  * function. The new status of a replica will be also recorded within the database.
  */
class VerifyJob
    :   public Job  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<VerifyJob> Ptr;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr)> CallbackType;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(Ptr,
                               ReplicaDiff const&,
                               std::vector<ReplicaDiff> const&)> CallbackTypeOnDiff;

   /// @return default options object for this type of a request
   static Job::Options const& defaultOptions();

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller          - for launching requests
     * @param parentJobId         - optional identifier of a parent job
     * @param onFinish            - callback function to be called upon a completion of the job
     @ @param onReplicaDifference - callback function to be called when two replicas won't match
     * @param maxReplicas         - (optional) maximum number of replicas to process simultaneously.
     *                              If the parameter is set to 0 (the default value) then 1 replica
     *                              will be assumed.
     * @param computeCheckSum     - (optional) tell a worker server to compute check/control sum on each file
     * @param onFinish            - (optional) callback function to be called upon a completion of the job
     * @param options             - (optional) job options
     *
     * @return poiter to the created object
     */
    static Ptr create(Controller::Ptr const& controller,
                      std::string const& parentJobId,
                      CallbackType onFinish,
                      CallbackTypeOnDiff onReplicaDifference,
                      size_t maxReplicas = 0,
                      bool computeCheckSum = false,
                      Job::Options const& options=defaultOptions());

    // Default construction and copy semantics are prohibited

    VerifyJob() = delete;
    VerifyJob(VerifyJob const&) = delete;
    VerifyJob& operator=(VerifyJob const&) = delete;

    ~VerifyJob() final = default;

    /// @return maximum number of replicas to be allowed processed simultaneously
    size_t maxReplicas() const { return _maxReplicas; }

    /// @return true if file check/control sums need to be recomputed
    bool computeCheckSum() const { return _computeCheckSum; }

    /**
     * @see Job::extendedPersistentState()
     */
    std::string extendedPersistentState(SqlGeneratorPtr const& gen) const override;

protected:

    /**
     * Construct the job with the pointer to the services provider.
     *
     * @see VerifyJob::create()
     */
    VerifyJob(Controller::Ptr const& controller,
              std::string const& parentJobId,
              CallbackType onFinish,
              CallbackTypeOnDiff onReplicaDifference,
              size_t maxReplicas,
              bool computeCheckSum,
              Job::Options const& options);

    /**
      * @see Job::startImpl()
      */
    void startImpl(util::Lock const& lock) final;

    /**
      * @see Job::startImpl()
      */
    void cancelImpl(util::Lock const& lock) final;

    /**
      * @see Job::notifyImpl()
      */
    void notifyImpl() final;

    /**
     * The calback function to be invoked on a completion of each request.
     *
     * @param request - a pointer to a request
     */
    void onRequestFinish(FindRequest::Ptr request);

    /**
     * Find the next replicas to be inspected and return 'true' if no suitable
     * candidates found. Normally the method should never return 'false' unless
     * no single replica exists in the system or there was a failure to find
     * replicas in the database.
     *
     * @param lock        - the lock must be acquired by a caller of the method
     * @param replicas    - a collection of replicas returned from the database
     * @param numReplicas - a desired number of replicas to be pulled from the database
     *
     * @return reslt of the search
     */
    bool nextReplicas(util::Lock const& lock,
                      std::vector<ReplicaInfo>& replicas,
                      size_t numReplicas);

protected:

    /// Client-defined function to be called upon the completion of the job
    CallbackType _onFinish;

    /// Client-defined function to be called when two replicas won't match
    CallbackTypeOnDiff _onReplicaDifference;

    /// The maximum number of replicas to be allowed processed simultaneously
    size_t _maxReplicas;

    /// This option will be passed on to the worker services
    bool _computeCheckSum;

    /// The current (last) batch if replicas which are being inspected.
    /// The replicas are registered by the corresponding request IDs.
    std::map<std::string, ReplicaInfo> _replicas;

    /// The current (last) batch of requests registered by their IDs
    std::map<std::string, FindRequest::Ptr> _requests;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_VERIFYJOB_H
