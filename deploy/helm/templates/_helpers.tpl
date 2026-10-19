{{/*
Expand the name of the chart.
*/}}
{{- define "qserv.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "qserv.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "qserv.labels" -}}
helm.sh/chart: {{ include "qserv.chart" . }}
{{ include "qserv.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "qserv.selectorLabels" -}}
app.kubernetes.io/name: {{ include "qserv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Validate the deployment mode, failing the render on a typo rather than silently
falling through to "full" (which would start the whole system when the operator
asked for a database-only or upgrade deployment).
*/}}
{{- define "qserv.mode" -}}
{{- $m := .Values.mode | default "full" -}}
{{- if not (has $m (list "full" "db-only" "mariadb-upgrade")) -}}
{{- fail (printf "invalid mode %q: expected one of full, db-only, mariadb-upgrade" $m) -}}
{{- end -}}
{{- $m -}}
{{- end }}

{{/*
Mode flags. Both "db-only" and "mariadb-upgrade" run the MariaDB containers alone,
with every Qserv sidecar gated off; "mariadb-upgrade" additionally replaces the
MariaDB container command with the upgrade script below.
*/}}
{{- define "qserv.enable" -}}
{{- $m := include "qserv.mode" . -}}
{{- if or (eq $m "db-only") (eq $m "mariadb-upgrade") -}}
czar: false
worker: false
registry: false
replication: false
{{- else -}} {{/* full */}}
czar: true
worker: true
registry: true
replication: true
{{- end -}}
{{- end }}

{{/*
True (a non-empty string) when running in "mariadb-upgrade" mode.
*/}}
{{- define "qserv.mariadbUpgrade" -}}
{{- if eq (include "qserv.mode" .) "mariadb-upgrade" -}}true{{- end -}}
{{- end }}

{{/*
The MariaDB container command used in "mariadb-upgrade" mode.

Runs mariadbd directly (bypassing the image's docker-entrypoint.sh, so that
upstream's MARIADB_AUTO_UPGRADE machinery stays out of the way), waits for it to
accept authenticated connections, then runs mariadb-upgrade with its *full* set of
passes. Deliberately omitting --upgrade-system-tables means mariadb-check runs over
all databases with --auto-repair and --process-views=UPGRADE, which is the point of
this mode: it remediates drift accumulated across MariaDB revisions, not just the
mysql.* system tables.

The pod parks with mariadbd still running once the upgrade finishes, so the
unmodified liveness/readiness probes keep passing. Pod phase is therefore NOT the
completion signal -- see the JSON marker and the QSERV-UPGRADE-RESULT log line.
*/}}
{{- define "qserv.mariadbUpgradeScript" -}}
set -uo pipefail

DATA=/qserv/data
MARKER="${DATA}/mysql-upgrade.json"
LOG="${DATA}/mysql-upgrade.log"
TMPDIR_UPGRADE="${DATA}/tmp"
STARTED="$(date -Is)"

# Authenticate with a single '$' here: unlike the probes below, this script is the
# container command, so plain shell expansion applies.
CONN="--protocol=TCP -h 127.0.0.1 -P3306 -uroot -p${MARIADB_ROOT_PASSWORD}"

log() { echo "[mariadb-upgrade] $*"; }

# Park indefinitely, leaving mariadbd up so the probes keep passing. Used on both
# success and failure: re-running repairs unattended on a multi-terabyte volume is
# more dangerous than a pod that looks healthy, and the marker records the truth.
park() {
    log "parking with mariadbd running; audit ${MARKER} to determine the outcome"
    while true; do sleep 3600; done
}

write_marker() { # status version checked repaired
    cat > "${MARKER}" <<MARKER_EOF
{"status":"$1","version":"$2","checked":$3,"repaired":$4,"started":"${STARTED}","finished":"$(date -Is)","pod":"${HOSTNAME:-unknown}"}
MARKER_EOF
}

mkdir -p "${TMPDIR_UPGRADE}"

# Keep repair scratch space on the data volume. The default tmpdir is the /tmp
# emptyDir, which a large MyISAM chunk-table repair could fill and get us evicted.
log "starting mariadbd (tmpdir=${TMPDIR_UPGRADE})"
mariadbd --tmpdir="${TMPDIR_UPGRADE}" &
MYSQLD_PID=$!

# Wait for authenticated access specifically. 'mariadb-admin ping' exits 0 even when
# the server answers with access-denied, so it cannot confirm we can actually log in.
log "waiting for mariadbd to accept connections"
for _ in $(seq 1 120); do
    if mariadb ${CONN} -N -B -e "SELECT 1" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "${MYSQLD_PID}" 2>/dev/null; then
        log "FATAL: mariadbd exited during startup"
        write_marker "failed-startup" "unknown" 0 0
        echo "QSERV-UPGRADE-RESULT status=failed-startup pod=${HOSTNAME:-unknown}"
        exit 1
    fi
    sleep 5
done

SERVER_VERSION="$(mariadb ${CONN} -N -B -e "SELECT VERSION()" 2>/dev/null)"
if [ -z "${SERVER_VERSION}" ]; then
    log "FATAL: could not query server version; check the root password"
    write_marker "failed-startup" "unknown" 0 0
    echo "QSERV-UPGRADE-RESULT status=failed-startup pod=${HOSTNAME:-unknown}"
    exit 1
fi
log "server is up, version ${SERVER_VERSION}"

# Idempotency: a completed run for this same server version is left alone, so the
# mode can be re-applied to finish a partially-completed multi-worker run.
if [ -f "${MARKER}" ] \
        && grep -q '"status":"ok"' "${MARKER}" \
        && grep -qF "${SERVER_VERSION}" "${MARKER}"; then
    log "marker already records a successful upgrade for ${SERVER_VERSION}; skipping"
    echo "QSERV-UPGRADE-RESULT status=already-done version=${SERVER_VERSION} pod=${HOSTNAME:-unknown}"
    park
fi
if [ -f "${MARKER}" ] && grep -q '"status":"failed' "${MARKER}"; then
    log "marker records a previous FAILED upgrade; refusing to retry automatically"
    echo "QSERV-UPGRADE-RESULT status=previously-failed pod=${HOSTNAME:-unknown}"
    park
fi

# Record an in-progress marker first, so a pod killed mid-repair is distinguishable
# from one that never started.
write_marker "running" "${SERVER_VERSION}" 0 0

log "running mariadb-upgrade (full check/repair of all databases; this may take a while)"
echo "=== $(date -Is) mariadb-upgrade ${SERVER_VERSION} on ${HOSTNAME:-unknown} ===" >> "${LOG}"
mariadb-upgrade ${CONN} --force 2>&1 | tee -a "${LOG}"
STATUS=${PIPESTATUS[0]}

# Approximate accounting scraped from the mariadb-check output: tables it reported on,
# and tables it actually had to repair. Full detail stays in ${LOG}.
# Note: 'grep -c' exits 1 on a zero count, so the count must be normalised rather
# than defaulted with '|| echo 0' (which would append a second zero).
count_matches() { # pattern
    local n
    n=$(grep -cE "$1" "${LOG}" 2>/dev/null) || n=0
    printf '%s' "${n:-0}"
}
CHECKED=$(count_matches '^[^[:space:]]+\.[^[:space:]]+[[:space:]]+')
REPAIRED=$(count_matches '^(Repairing|Fixing)')
ELAPSED_NOTE="see ${LOG}"

if [ "${STATUS}" -eq 0 ]; then
    write_marker "ok" "${SERVER_VERSION}" "${CHECKED}" "${REPAIRED}"
    log "upgrade completed successfully (${ELAPSED_NOTE})"
    echo "QSERV-UPGRADE-RESULT status=ok version=${SERVER_VERSION} checked=${CHECKED} repaired=${REPAIRED} pod=${HOSTNAME:-unknown}"
else
    write_marker "failed" "${SERVER_VERSION}" "${CHECKED}" "${REPAIRED}"
    log "UPGRADE FAILED with status ${STATUS} (${ELAPSED_NOTE})"
    echo "QSERV-UPGRADE-RESULT status=failed exit=${STATUS} version=${SERVER_VERSION} checked=${CHECKED} repaired=${REPAIRED} pod=${HOSTNAME:-unknown}"
fi

park
{{- end }}
