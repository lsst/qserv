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
Validate the deployment mode, failing the render on a typo rather than silently falling through to "full"
(which would start the whole system when the operator asked for a database-only or upgrade deployment).
*/}}
{{- define "qserv.mode" -}}
{{- $m := .Values.mode | default "full" -}}
{{- if not (has $m (list "full" "db-only" "mariadb-upgrade")) -}}
{{- fail (printf "invalid mode %q: expected one of full, db-only, mariadb-upgrade" $m) -}}
{{- end -}}
{{- $m -}}
{{- end }}

{{/*
Mode flags. Both "db-only" and "mariadb-upgrade" run the MariaDB containers alone, with every Qserv container
gated off; "mariadb-upgrade" additionally replaces the MariaDB container command with the upgrade script
below.
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
The MariaDB container command used in "mariadb-upgrade" mode: ../files/mariadb-upgrade.sh, inlined verbatim
into the czar/worker/repl StatefulSets. See that file for what it does and why. It takes no template
substitutions -- .Values.mariadbUpgrade.force reaches it as the QSERV_MARIADB_UPGRADE_FORCE environment
variable, set alongside MARIADB_ROOT_PASSWORD in each StatefulSet -- so it is plain bash that shellcheck and
"bash -n" can read, and it is deliberately NOT passed through tpl.
*/}}
{{- define "qserv.mariadbUpgradeScript" -}}
{{- .Files.Get "files/mariadb-upgrade.sh" | trimSuffix "\n" -}}
{{- end }}

{{/*
The extra MariaDB container environment needed in "mariadb-upgrade" mode. Rendered as list items, to be
appended to the container's existing env.
*/}}
{{- define "qserv.mariadbUpgradeEnv" -}}
- name: QSERV_MARIADB_UPGRADE_FORCE
  value: {{ (.Values.mariadbUpgrade | default dict).force | default false | quote }}
{{- end }}
