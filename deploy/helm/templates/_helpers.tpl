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
Mode flags
*/}}
{{- define "qserv.enable" -}}
{{- $m := .Values.mode | default "full" -}}
{{- if eq $m "db-only" -}}
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
