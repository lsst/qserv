#!/bin/bash
# This is the MariaDB container command used in the Helm chart's "mariadb-upgrade" mode. Inlined verbatim into
# the czar/worker/repl StatefulSets by the "qserv.mariadbUpgradeScript" helper; this file is the only copy.
#
# Runs mariadbd directly (bypassing the image's docker-entrypoint.sh, so that upstream's MARIADB_AUTO_UPGRADE
# machinery stays out of the way -- which relies on the container running as a non-root uid, provided by the
# image's "USER qserv" and the StatefulSets' runAsUser: 1000, because mariadbd refuses to run as root), waits
# for it to accept authenticated connections, then runs mariadb-upgrade with its *full* set of passes.
# Deliberately omitting --upgrade-system-tables means mariadb-check runs over all databases with --auto-repair
# and --process-views=UPGRADE, which is the point of this mode: update everything, not just the mysql.* system
# tables.
#
# The design rule is deliberately blunt: succeed quietly on the simple path, and park for a human on
# everything else. Anything that is not a clean success, a clean recorded failure, or an already-upgraded
# volume -- a run that never finished, a marker that cannot be read or written, a log that cannot be written
# -- parks with "status=needs-attention reason=..." instead of guessing. Nothing is retried unattended,
# because re-running repairs on a multi-terabyte volume is not a decision this script should make.
#
# Every terminal state parks with mariadbd still running, so the unmodified liveness/readiness probes keep
# passing. Pod phase is therefore NOT the completion signal -- read the status marker file written to each
# data volume (one line of key=value fields; see write_marker below) or the QSERV-UPGRADE-RESULT log line.

set -uo pipefail

DATA=/qserv/data
MARKER="${DATA}/mysql-upgrade.status"
LOG="${DATA}/mysql-upgrade.log"
TMPDIR_UPGRADE="${DATA}/tmp"
STARTED="$(date -Is)"

# Set from .Values.mariadbUpgrade.force by the StatefulSets: when true, run regardless of what the marker
# says. This is the way to clear any volume parked below -- a recorded failure, a run that never finished, an
# unreadable marker -- as a values change plus a redeploy, rather than a human exec'ing into every pod to
# delete marker files by hand. It applies to every volume in the release; there is no way to scope it to a
# single pod or component.
FORCE="${QSERV_MARIADB_UPGRADE_FORCE:-false}"

# Authenticate with a single '$': unlike the probes in the StatefulSets, this script runs as the container
# command, so plain shell expansion applies. An array expanded as "${CONN[@]}", so that a root password
# containing whitespace or glob characters survives as a single argument.
CONN=(--protocol=TCP -h 127.0.0.1 -P3306 -uroot "-p${MARIADB_ROOT_PASSWORD}")

log() { echo "[mariadb-upgrade] $*"; }

# The one machine-readable line the audit greps for; everything else this script prints is prose.
result() { # status [extra-field ...]
    local status="$1"
    shift
    local extra="$*"
    echo "QSERV-UPGRADE-RESULT status=${status} ${extra:+${extra} }version=${SERVER_VERSION:-unknown} pod=${HOSTNAME:-unknown}"
}

# Stop the server we started and wait for it to close its data files. A no-op if it has already exited.
stop_mariadbd() {
    log "stopping mariadbd"
    kill -TERM "${MYSQLD_PID}" 2>/dev/null
    wait "${MYSQLD_PID}" 2>/dev/null
}

# This script, not mariadbd, is PID 1 here (the image entrypoint is bypassed), so termination signals land on
# bash and mariadbd would otherwise be SIGKILLed when the grace period expires. Forward the signal and wait
# for the server to close its data files. Note that bash defers trap handlers until a running foreground
# command returns: this covers the parked state, NOT a teardown issued while mariadb-upgrade is still
# repairing. Audit completion before deleting pods.
shutdown_and_exit() {
    log "received termination signal"
    stop_mariadbd
    log "mariadbd exited; done"
    exit 0
}

# Park indefinitely, leaving mariadbd up so the probes keep passing. Every terminal state ends here.
# 'sleep & wait' rather than a plain 'sleep' so the shutdown trap above runs promptly instead of waiting out
# the current sleep.
park() {
    log "parking with mariadbd running; audit ${MARKER} to determine the outcome"
    while true; do sleep 3600 & wait $!; done
}

# Park asking for a human. Used for everything this script declines to decide on its own.
needs_attention() { # reason
    log "NEEDS ATTENTION: $1 -- doing nothing further to this volume; investigate, then redeploy with" \
        "mariadbUpgrade.force set to retry"
    result needs-attention "reason=$1"
    park
}

# The durable record the audit reads: one line of space-separated key=value fields, the same vocabulary as
# the QSERV-UPGRADE-RESULT line above. Not JSON -- nothing parses this as JSON, and a format that merely
# looks structured invites being read with substring matches, which is how a partial file ends up
# authorising a decision. Field splitting is safe because no value can contain a space (a status word, a
# MariaDB version string, ISO-8601 timestamps, a pod name); one that somehow did would fail the field count
# in read_marker below and park.
#
# Staged alongside and renamed, so a reader never sees a half-written marker; a write that fails -- most
# plausibly the data volume filling, since the repair scratch space lives on it -- is reported to the caller
# rather than ignored.
write_marker() { # status
    local tmp="${MARKER}.tmp"
    if printf 'status=%s version=%s started=%s finished=%s pod=%s\n' \
            "$1" "${SERVER_VERSION}" "${STARTED}" "$(date -Is)" "${HOSTNAME:-unknown}" > "${tmp}" \
            && mv -f "${tmp}" "${MARKER}"; then
        return 0
    fi
    rm -f "${tmp}" 2>/dev/null
    return 1
}

# Read the marker by splitting it into fields, so the decision below is made from parsed values rather than
# from patterns found somewhere in the file. Exactly one line of exactly the five fields write_marker emits,
# or this is not a record we wrote: truncated, concatenated, reordered and extra-field files all fail here.
read_marker() { # -> MARKER_STATUS, MARKER_VERSION; non-zero if the file is not one of our records
    local f1="" f2="" f3="" f4="" f5="" extra="" second=""
    { read -r f1 f2 f3 f4 f5 extra; read -r second; } < "${MARKER}"
    [ -z "${extra}" ] && [ -z "${second}" ] || return 1
    [[ ${f1} == status=ok || ${f1} == status=running || ${f1} == status=failed ]] || return 1
    [[ ${f2} == version=?* && ${f3} == started=?* && ${f4} == finished=?* && ${f5} == pod=?* ]] || return 1
    MARKER_STATUS="${f1#status=}"
    MARKER_VERSION="${f2#version=}"
}

mkdir -p "${TMPDIR_UPGRADE}"

# Keep repair scratch space on the data volume. The default tmpdir is the /tmp emptyDir, which a large
# MyISAM chunk-table repair could fill and get us evicted.
log "starting mariadbd (tmpdir=${TMPDIR_UPGRADE})"
mariadbd --tmpdir="${TMPDIR_UPGRADE}" &
MYSQLD_PID=$!
trap shutdown_and_exit TERM INT

# Wait for authenticated access specifically. 'mariadb-admin ping' exits 0 even when the server answers with
# access-denied, so it cannot confirm we can actually log in.
log "waiting for mariadbd to accept connections"
for _ in $(seq 1 120); do
    if mariadb "${CONN[@]}" -N -B -e "SELECT 1" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "${MYSQLD_PID}" 2>/dev/null; then
        # Startup failures are the one case that does not park: they carry no version, represent no repair
        # work, and are usually transient (bad secret, slow volume attach), so let the crashloop show rather
        # than hiding it behind a healthy-looking parked pod.
        log "FATAL: mariadbd exited during startup"
        result failed-startup
        exit 1
    fi
    sleep 5
done

SERVER_VERSION="$(mariadb "${CONN[@]}" -N -B -e "SELECT VERSION()" 2>/dev/null)"
if [ -z "${SERVER_VERSION}" ]; then
    log "FATAL: could not query server version; check the root password"
    result failed-startup
    stop_mariadbd
    exit 1
fi
log "server is up, version ${SERVER_VERSION}"

# One rule, keyed on the *running* server version: either this volume is already recorded as upgraded for
# this version, or it has no marker for this version and we upgrade it, or a human looks at it. A marker
# left by an earlier version describes that version, so it is ignored and no cleanup pass is needed before
# the next major upgrade -- the marker self-invalidates when VERSION() changes.
if [ "${FORCE}" = "true" ]; then
    log "mariadbUpgrade.force is set; ignoring any existing marker"
elif [ -f "${MARKER}" ]; then
    if ! read_marker; then
        needs_attention "marker-unreadable"
    elif [ "${MARKER_VERSION}" != "${SERVER_VERSION}" ]; then
        log "marker is from server version ${MARKER_VERSION} and does not apply to ${SERVER_VERSION}"
    elif [ "${MARKER_STATUS}" = "ok" ]; then
        log "marker already records a successful upgrade for ${SERVER_VERSION}"
        result already-done
        park
    else
        # Same version, not "ok": a recorded failure, or a run that never finished because the pod was
        # killed mid-repair. Both want a human before repairs run on this volume again.
        needs_attention "previous-run-did-not-complete"
    fi
fi

# Record an in-progress marker first, so a pod killed mid-repair leaves something for the rule above to
# park on next time.
write_marker "running" || needs_attention "marker-not-writable"

log "running mariadb-upgrade (full check/repair of all databases; this may take a while)"
echo "=== $(date -Is) mariadb-upgrade ${SERVER_VERSION} on ${HOSTNAME:-unknown} ===" >> "${LOG}" \
    || needs_attention "log-not-writable"
mariadb-upgrade "${CONN[@]}" --force 2>&1 | tee -a "${LOG}"
# Copy the whole array before reading it: a plain assignment is itself a pipeline and would reset PIPESTATUS.
# tee exits non-zero if it cannot write its file, and an outcome whose detail nobody can read is not a
# result worth recording -- park instead.
PIPE=("${PIPESTATUS[@]}")
STATUS=${PIPE[0]}
[ "${PIPE[1]}" -eq 0 ] || needs_attention "log-write-failed"

if [ "${STATUS}" -eq 0 ]; then
    log "upgrade completed successfully (detail in ${LOG})"
    write_marker "ok" || needs_attention "upgrade-ok-but-marker-not-writable"
    result ok
else
    log "UPGRADE FAILED with status ${STATUS} (detail in ${LOG})"
    write_marker "failed" || needs_attention "upgrade-failed-and-marker-not-writable"
    result failed "exit=${STATUS}"
fi

park
