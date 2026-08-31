#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cell_script="${repo_root}/scripts/run-capacity-cell.sh"

command -v jq >/dev/null 2>&1 || { printf 'jq is required\n' >&2; exit 2; }

screen_id="${CAPACITY_OUTBOX_SCREEN_ID:-outbox-batch-screen-$(date -u +%Y%m%dT%H%M%SZ)}"
if ! [[ "${screen_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  printf 'CAPACITY_OUTBOX_SCREEN_ID must contain only letters, digits, dot, underscore, or dash\n' >&2
  exit 2
fi

rate="${CAPACITY_OUTBOX_SCREEN_RATE:-1500}"
payload="${CAPACITY_OUTBOX_SCREEN_PAYLOAD:-mixed}"
warmup="${CAPACITY_OUTBOX_SCREEN_WARMUP:-30s}"
measured="${CAPACITY_OUTBOX_SCREEN_DURATION:-60s}"
drain="${CAPACITY_OUTBOX_SCREEN_DRAIN_TIMEOUT:-30s}"
checkpoint="${CAPACITY_OUTBOX_SCREEN_CHECKPOINT_TIMEOUT:-60s}"

if ! [[ "${rate}" =~ ^[1-9][0-9]*$ ]]; then
  printf 'CAPACITY_OUTBOX_SCREEN_RATE must be a positive integer, got %q\n' "${rate}" >&2
  exit 2
fi
case "${payload}" in
  small|mixed) ;;
  *)
    printf 'CAPACITY_OUTBOX_SCREEN_PAYLOAD must be small or mixed, got %q\n' "${payload}" >&2
    exit 2
    ;;
esac

screen_root="${CAPACITY_OUTBOX_SCREEN_ROOT:-${repo_root}/tmp/capacity/screens}"
if [[ "${screen_root}" != /* ]]; then
  screen_root="${repo_root}/${screen_root}"
fi
screen_dir="${screen_root}/${screen_id}"
runs_root="${screen_dir}/runs"
summary_file="${screen_dir}/screen.json"
markdown_file="${screen_dir}/screen.md"
if [[ -e "${screen_dir}" ]]; then
  printf 'screen ID %s already has artifacts; choose a new CAPACITY_OUTBOX_SCREEN_ID\n' "${screen_id}" >&2
  exit 2
fi
mkdir -p "${runs_root}"

printf '\n==> GoMessenger batch integration preflight\n'
make -C "${repo_root}" test-batch-integration

git_commit="$(git -C "${repo_root}" rev-parse HEAD 2>/dev/null || printf unknown)"
git_dirty=false
if [[ -n "$(git -C "${repo_root}" status --porcelain 2>/dev/null)" ]]; then
  git_dirty=true
fi
outbox_root="$(cd "${repo_root}/../outbox" 2>/dev/null && pwd || true)"
if [[ -z "${outbox_root}" || ! -d "${outbox_root}/.git" ]]; then
  printf 'The checkout-local Outbox screen requires ../outbox\n' >&2
  exit 2
fi
outbox_git_commit="$(git -C "${outbox_root}" rev-parse HEAD)"
outbox_git_dirty=false
if [[ -n "$(git -C "${outbox_root}" status --porcelain)" ]]; then
  outbox_git_dirty=true
fi

roles=(control candidate)
variants=(consumer-batch full-batch)
run_status=0

for index in "${!roles[@]}"; do
  role="${roles[index]}"
  variant="${variants[index]}"
  run_id="${screen_id}-${role}"
  report_file="${runs_root}/${run_id}/report.json"

  printf '\n==> Outbox screen role=%s variant=%s rate=%s payload=%s\n' \
    "${role}" "${variant}" "${rate}" "${payload}"
  set +e
  CAPACITY_RUN_ID="${run_id}" \
  CAPACITY_RESULTS_ROOT="${runs_root}" \
  CAPACITY_CELL_RATE="${rate}" \
  CAPACITY_CELL_VARIANT="${variant}" \
  CAPACITY_CELL_TOPOLOGY=o2-c2 \
  CAPACITY_PAYLOAD_PROFILE="${payload}" \
  CAPACITY_POSTGRES_PROFILE=stock \
  CAPACITY_PRECONDITION_DURATION="${warmup}" \
  CAPACITY_MEASURED_DURATION="${measured}" \
  CAPACITY_DRAIN_TIMEOUT="${drain}" \
  CAPACITY_CHECKPOINT_TIMEOUT="${checkpoint}" \
  CAPACITY_PPROF_SECONDS=10 \
  CAPACITY_MIN_RATE=0 \
  CAPACITY_OUTBOX_BATCH_MAX_MESSAGES=100 \
  CAPACITY_CONSUMER_BATCH_MAX_MESSAGES=100 \
  bash "${cell_script}"
  command_status=$?
  set -e

  if [[ ! -f "${report_file}" ]]; then
    printf 'screen report is missing: %s\n' "${report_file}" >&2
    exit 1
  fi
  if (( command_status != 0 )); then
    run_status=1
  fi

	compose_log="${runs_root}/${run_id}/compose.log"
	if [[ ! -f "${compose_log}" ]]; then
		printf 'screen compose log is missing: %s\n' "${compose_log}" >&2
		exit 1
	fi
	canceling_statements="$(grep -Fic 'canceling statement due to user request' "${compose_log}" || true)"
	broken_pipes="$(grep -Fic 'Broken pipe' "${compose_log}" || true)"
	connection_resets="$(grep -Fic 'Connection reset by peer' "${compose_log}" || true)"
	client_connections_lost="$(grep -Fic 'connection to client lost' "${compose_log}" || true)"
	connection_churn_total="$((canceling_statements + broken_pipes + connection_resets + client_connections_lost))"

  jq \
    --arg role "${role}" \
    --arg variant "${variant}" \
    --argjson commandStatus "${command_status}" \
    --arg reportPath "${report_file}" \
		--argjson cancelingStatements "${canceling_statements}" \
		--argjson brokenPipes "${broken_pipes}" \
		--argjson connectionResets "${connection_resets}" \
		--argjson clientConnectionsLost "${client_connections_lost}" \
		--argjson connectionChurnTotal "${connection_churn_total}" \
    '
      .stages[0] as $stage |
      ($stage.loadWindow.committed // 0) as $committed |
      {
        role:$role,
        variant:$variant,
        commandStatus:$commandStatus,
        runId:.runId,
        startedAt:.startedAt,
        completedAt:.completedAt,
        failure:(.failure // null),
        config:{
          rate:($stage.targetRate // .config.rates[0]),
          payload:.config.payloadProfile,
          warmupSeconds:.config.warmupSeconds,
          measuredSeconds:.config.stageSeconds,
          drainTimeoutSeconds:.config.drainTimeoutSeconds
        },
        provenance:{
          gitCommit:.environment.gitCommit,
          gitDirty:.environment.gitDirty,
          outboxGitCommit:.environment.outboxGitCommit,
          outboxGitDirty:.environment.outboxGitDirty,
          outboxVersion:.environment.outboxVersion,
          outboxIngressMode:.environment.outboxIngressMode,
          outboxRelayMode:.environment.outboxRelayMode,
          consumerMode:.environment.consumerMode
        },
        integrityPassed:.integrityPassed,
        sustainable:($stage.sustainable // false),
        unsustainableReasons:($stage.unsustainableReasons // []),
				connectionHealth:($stage.outboxDatabase // null),
				connectionChurn:{
					cancelingStatements:$cancelingStatements,
					brokenPipes:$brokenPipes,
					connectionResets:$connectionResets,
					clientConnectionsLost:$clientConnectionsLost,
					total:$connectionChurnTotal
				},
        metrics:{
          acceptedMessagesPerSecond:$stage.acceptedMessagesPerSecond,
          relayMessagesPerSecond:$stage.relayMessagesPerSecond,
          consumerMessagesPerSecond:$stage.consumerMessagesPerSecond,
          businessP95Millis:$stage.latency.p95Millis,
          drainSeconds:$stage.drainSeconds,
          sqlCallsPerMessage:(if $committed > 0 then $stage.postgresqlNormalized.sqlCalls / $committed else null end),
          transactionsPerMessage:$stage.postgresqlNormalized.transactionsPerMessage,
          walBytesPerMessage:$stage.postgresqlNormalized.walBytesPerMessage,
          outboxAverageBatch:$stage.outboxExecution.handler.averageMessages,
          outboxPublishAverageBatch:$stage.outboxExecution.publish.averageMessages,
          outboxFinalizationAverageBatch:$stage.outboxExecution.finalization.averageMessages,
          consumerAverageBatch:$stage.consumerBatch.averageMessages,
          retry:$stage.outboxExecution.outcomes.retry,
          defer:$stage.outboxExecution.outcomes.defer,
          dlq:$stage.outboxExecution.outcomes.dlq
        },
        report:$reportPath
      }
    ' \
    "${report_file}" > "${screen_dir}/${role}.json"
done

jq -s \
  --arg screenId "${screen_id}" \
  --arg gitCommit "${git_commit}" \
  --argjson gitDirty "${git_dirty}" \
  --arg outboxGitCommit "${outbox_git_commit}" \
  --argjson outboxGitDirty "${outbox_git_dirty}" \
  '
    .[0] as $control |
    .[1] as $candidate |
    def ratio($base; $value):
      if $base == null or $base == 0 or $value == null then null else $value / $base end;
		def poolHealthy($pool):
			$pool != null and
			$pool.maxConnections > 0 and
			$pool.maxAcquiredConnections > 0 and
			$pool.maxAcquiredConnections <= $pool.maxConnections and
			$pool.replacementConnections == 0 and
			$pool.canceledAcquires == 0 and
			$pool.unusableReleases == 0;
		def runConnectionHealthy:
			.connectionChurn.total == 0 and
			poolHealthy(.connectionHealth.producer) and
			poolHealthy(.connectionHealth.relay);
    {
      specVersion:"2.1-outbox-batch-screen-1",
      screenId:$screenId,
      evidenceScope:"development-screen",
      verdict:"SCREEN_ONLY",
      disclaimer:"Short dirty-checkout screening evidence; not a capacity proof or publication claim.",
			screenComplete:(
				all(.[]; .commandStatus == 0 and .integrityPassed == true and runConnectionHealthy) and
				$candidate.sustainable == true and
				$candidate.metrics.outboxAverageBatch >= 10
			),
      checkout:{
        gitCommit:$gitCommit,
        gitDirty:$gitDirty,
        outboxGitCommit:$outboxGitCommit,
        outboxGitDirty:$outboxGitDirty
      },
      runs:.,
      comparison:{
        relayThroughputRatio:ratio($control.metrics.relayMessagesPerSecond; $candidate.metrics.relayMessagesPerSecond),
        businessP95Ratio:ratio($control.metrics.businessP95Millis; $candidate.metrics.businessP95Millis),
        sqlCallsPerMessageRatio:ratio($control.metrics.sqlCallsPerMessage; $candidate.metrics.sqlCallsPerMessage),
        transactionsPerMessageRatio:ratio($control.metrics.transactionsPerMessage; $candidate.metrics.transactionsPerMessage),
        walBytesPerMessageRatio:ratio($control.metrics.walBytesPerMessage; $candidate.metrics.walBytesPerMessage),
        candidateOutboxAverageBatch:$candidate.metrics.outboxAverageBatch
      }
    }
  ' "${screen_dir}/control.json" "${screen_dir}/candidate.json" > "${summary_file}"

jq -r '
  def f2: if . == null then "n/a" else ((. * 100 | round) / 100 | tostring) end;
  .runs[0] as $control |
  .runs[1] as $candidate |
  "# Outbox batch development screen\n\n" +
  "Screen ID: `\(.screenId)`\n\n" +
  "Evidence scope: `\(.evidenceScope)`; verdict: `\(.verdict)`.\n\n" +
	"Screen gate (both integrity and zero churn; candidate sustainable with average Outbox batch >= 10): `\(.screenComplete)`.\n\n" +
  "This short run is not a capacity proof and cannot support a `>=1.3x` claim.\n\n" +
  "Configuration: `\($control.config.rate) msg/s`, `\($control.config.payload)`, " +
  "warm-up `\($control.config.warmupSeconds)s`, measured `\($control.config.measuredSeconds)s`.\n\n" +
	"| Role | Variant | Sustainable | Log churn | Pool replacements | Relay msg/s | Consumer msg/s | Business p95 ms | tx/msg | WAL B/msg | Outbox avg batch |\n" +
	"|---|---|:---:|---:|---:|---:|---:|---:|---:|---:|---:|\n" +
	"| control | \($control.variant) | \($control.sustainable) | \($control.connectionChurn.total) | \($control.connectionHealth.producer.replacementConnections + $control.connectionHealth.relay.replacementConnections) | \($control.metrics.relayMessagesPerSecond | f2) | " +
  "\($control.metrics.consumerMessagesPerSecond | f2) | \($control.metrics.businessP95Millis | f2) | " +
  "\($control.metrics.transactionsPerMessage | f2) | \($control.metrics.walBytesPerMessage | f2) | " +
  "\($control.metrics.outboxAverageBatch | f2) |\n" +
	"| candidate | \($candidate.variant) | \($candidate.sustainable) | \($candidate.connectionChurn.total) | \($candidate.connectionHealth.producer.replacementConnections + $candidate.connectionHealth.relay.replacementConnections) | \($candidate.metrics.relayMessagesPerSecond | f2) | " +
  "\($candidate.metrics.consumerMessagesPerSecond | f2) | \($candidate.metrics.businessP95Millis | f2) | " +
  "\($candidate.metrics.transactionsPerMessage | f2) | \($candidate.metrics.walBytesPerMessage | f2) | " +
  "\($candidate.metrics.outboxAverageBatch | f2) |\n\n" +
  (if .screenComplete then "" else
    "Failed screen gate details:\n\n" +
    ([$control, $candidate] | map(
      "- `\(.role)`: command status `\(.commandStatus)`, integrity `\(.integrityPassed)`, " +
      "sustainable `\(.sustainable)`, log churn `\(.connectionChurn.total)`, " +
      "pool replacements `\(.connectionHealth.producer.replacementConnections + .connectionHealth.relay.replacementConnections)`, " +
      "Outbox avg batch `\(.metrics.outboxAverageBatch | f2)`, " +
      "failure `\(.failure // "not reported")`, reasons `\(.unsustainableReasons | join("; "))`."
    ) | join("\n")) + "\n\n"
  end) +
  "GoMessenger `\(.checkout.gitCommit)` (dirty: `\(.checkout.gitDirty)`); " +
  "Outbox `\(.checkout.outboxGitCommit)` (dirty: `\(.checkout.outboxGitDirty)`).\n"
' "${summary_file}" > "${markdown_file}"

if ! jq -e '
	def poolHealthy($pool):
		$pool != null and
		$pool.maxConnections > 0 and
		$pool.maxAcquiredConnections > 0 and
		$pool.maxAcquiredConnections <= $pool.maxConnections and
		$pool.replacementConnections == 0 and
		$pool.canceledAcquires == 0 and
		$pool.unusableReleases == 0;
	all(.runs[];
		.commandStatus == 0 and
		.integrityPassed == true and
		.connectionChurn.total == 0 and
		poolHealthy(.connectionHealth.producer) and
		poolHealthy(.connectionHealth.relay)
	) and
  .runs[0].provenance.outboxIngressMode == "single" and
  .runs[0].provenance.outboxRelayMode == "single" and
  .runs[0].provenance.consumerMode == "batch" and
  .runs[1].provenance.outboxIngressMode == "batch" and
  .runs[1].provenance.outboxRelayMode == "batch" and
  .runs[1].provenance.consumerMode == "batch" and
	.runs[1].sustainable == true and
	.runs[1].metrics.outboxAverageBatch >= 10
' "${summary_file}" >/dev/null; then
	printf 'Outbox screen failed integrity, connection-health, candidate-sustainability, runtime-mode, or exercised-batch checks\n' >&2
  run_status=1
fi

printf '\nOutbox development screen: %s\n' "${summary_file}"
printf 'Compact screen report: %s\n' "${markdown_file}"
printf 'Verdict: SCREEN_ONLY (not a capacity proof)\n'
exit "${run_status}"
