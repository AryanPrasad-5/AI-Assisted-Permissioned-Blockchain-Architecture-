"""Build the machine-readable Phase 3 shadow-only comparison package."""
from __future__ import annotations
import argparse, csv, hashlib, json, shutil, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()

def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]

def dump(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True)+'\n', encoding='utf-8')

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--evidence-dir', type=Path, required=True)
    args=ap.parse_args()
    out=args.evidence_dir.resolve()
    replay=out/'controller_replay'
    root=ROOT
    decisions=read_jsonl(replay/'controller_decisions.jsonl')
    frames=read_jsonl(root/'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/controller_frames.jsonl')
    manifest=json.loads((replay/'integration_manifest.json').read_text(encoding='utf-8-sig'))
    capture=json.loads((root/'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/evidence_summary.json').read_text(encoding='utf-8-sig'))
    assert len(frames)==2 and len(decisions)==6
    assert manifest['mode']=='shadow' and manifest['live_submission_enabled'] is False
    assert manifest['identical_input_digests'] is True
    for idx in range(len(frames)):
        paired=[r for r in decisions if r['integration']['decision_index']==idx]
        assert len(paired)==3 and len({r['integration']['input_digest'] for r in paired})==1
    for frame in frames:
        origin=frame['state']['timestamp_utc']
        assert frame['state']['arrival_history_timestamps_utc']==sorted(frame['state']['arrival_history_timestamps_utc'])
        assert all(t<=origin for t in frame['state']['arrival_history_timestamps_utc'])
        assert all(ts<=frame['observation_timestamp_utc'] for ts in frame['source_timestamps_utc'].values())
    assert all(r['application'] is None and not r['integration']['live_submission_enabled'] for r in decisions)
    strategies=sorted({r['decision']['controller'] for r in decisions})
    assert strategies==['predictive','reactive','static']
    shutil.copyfile(replay/'controller_decisions.jsonl', out/'controller_decisions.jsonl')

    rows=[]
    for rec in decisions:
        d=rec['decision']; i=rec['integration']; state=rec['state']
        rows.append({
            'paired_trace_id':'phase2a3-c001','repetition':1,'frame_index':i['decision_index'],
            'observation_timestamp_utc':state['timestamp_utc'],'input_digest':i['input_digest'],
            'strategy':d['controller'],'mode':'shadow','initial_max_message_count':40,
            'observed_arrival_tps':state.get('observed_arrival_rate_tps'),
            'observed_valid_throughput_tps':state.get('observed_throughput_tps'),
            'observed_mean_latency_ms':state.get('observed_latency_ms'),
            'observed_p95_latency_ms':state.get('observed_latency_p95_ms'),
            'observed_queue_depth':state.get('queue_depth'),
            'recommended_max_message_count':d['config'].get('max_message_count'),
            'recommendation_changed':d.get('changed'), 'decision_reason':d.get('reason'),
            'predicted_arrival_tps':d.get('predicted_arrival_rate_tps'),
            'predicted_throughput_tps':(d.get('predicted_performance') or {}).get('throughput_tps'),
            'predicted_latency_ms':(d.get('predicted_performance') or {}).get('latency_ms'),
            'decision_overhead_ms':d.get('decision_overhead_ms'),
            'decision_duration_ms':i.get('decision_monotonic_duration_ms'),
            'prediction_error':d.get('metadata',{}).get('error'),
            'actuator_application':rec.get('application'),
            'live_submission_enabled':i.get('live_submission_enabled'),
        })
    with (out/'paired_run_results.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)

    perf=capture['metrics']
    perf_row={
        'source_run_id':capture['run_id'],'channel_id':capture['channel'],
        'measurement_scope':'single shared observed Fabric capture; not attributable to any controller',
        'measurement_window_s':5.0,'submitted':perf['submitted'],'committed_valid':perf['valid'],
        'failed':perf['failed'],'invalid':perf['invalid'],'pending_unresolved':perf['pending_unresolved'],
        'offered_tps':perf['offered_tps'],'delivered_valid_tps':perf['delivered_valid_tps'],
        'mean_latency_ms':perf['mean_client_latency_ms'],'p95_latency_ms':perf['p95_client_latency_ms'],
        'ledger_verification_coverage':perf['ledger_verification_coverage'],
        'commit_timestamp_quality':'peer block observation timestamp; exact commit timestamp unavailable',
        'source_evidence':'Phase 2A.3 evidence_summary.json; 21/21 transaction IDs joined to independently verified ledger rows',
    }
    with (out/'performance_metrics.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(perf_row)); w.writeheader(); w.writerow(perf_row)

    grouped={s:[r for r in decisions if r['decision']['controller']==s] for s in strategies}
    decision_summary={}
    for s,rs in grouped.items():
        durations=[float(r['integration']['decision_monotonic_duration_ms']) for r in rs]
        reasons={}
        for r in rs: reasons[r['decision']['reason']]=reasons.get(r['decision']['reason'],0)+1
        decision_summary[s]={
            'frames':len(rs),'recommendation_changes':sum(bool(r['decision']['changed']) for r in rs),
            'reasons':reasons,'decision_duration_ms':durations,
            'median_decision_duration_ms':statistics.median(durations),
            'recommended_values':[r['decision']['config']['max_message_count'] for r in rs],
        }
    dump(out/'statistical_comparisons.json',{
      'status':'NOT_ESTIMABLE_FOR_CONTROLLER_PERFORMANCE',
      'protocol_unit':'paired run within fixed regime and trace',
      'available_paired_runs':1,
      'independent_workload_traces':1,
      'available_observed_fabric_captures':1,
      'eligible_regime_strata':[],
      'controller_performance_contrasts':[],
      'reason':'No strategy was applied to Fabric; the one observed Fabric capture is shared context, not an arm outcome. Only one trace/capture and two dependent decision frames are available, below preregistered five-repetition pilot minimum.',
      'bootstrap_ci':None,'cliffs_delta':None,'paired_effect_size':None,'tost':None,'multiplicity_correction':'not applicable',
      'decision_behavior_descriptives_only':decision_summary,
      'warning':'Do not interpret decision latency/reasons or HFBTP utility as observed performance.'
    })
    predictive=[r for r in decisions if r['decision']['controller']=='predictive']
    summary={
      'experiment_id':'phase3_three_controller_comparison_20261111','verdict':'INCONCLUSIVE — EVIDENCE INSUFFICIENT',
      'execution_mode':'shadow_replay_only','live_submission_enabled':False,'actuator_invocations':0,
      'run_id':manifest['run_id'],'trace_id':'phase2a3-c001','repetitions_per_strategy':1,
      'eligible_input_frames':len(frames),'controller_records':len(decisions),'strategies':strategies,
      'identical_input_digests':manifest['identical_input_digests'],'stop_reasons':manifest['stop_reasons'],
      'all_recommendations_unchanged':not any(r['decision']['changed'] for r in decisions),
      'predictive_out_of_domain_abstentions':sum(r['decision']['reason']=='prediction_failure_fail_safe' for r in predictive),
      'regimes_evaluated':[],
      'measured_controller_performance_comparison':False,
      'shared_observed_fabric_capture':{
        'run_id':capture['run_id'],'channel':capture['channel'],'valid_transactions':perf['valid'],
        'failed':perf['failed'],'invalid':perf['invalid'],'offered_tps':perf['offered_tps'],
        'delivered_valid_tps':perf['delivered_valid_tps'],'mean_latency_ms':perf['mean_client_latency_ms'],
        'p95_latency_ms':perf['p95_client_latency_ms'],'source':'Phase 2A.3 contemporaneous evidence',
        'arm_attribution':'none'
      },
      'protocol_deviation':'Shadow-only safety boundary precludes required live paired runs and measured arm outcomes; see pre_run_readiness_and_deviation.md.',
      'interpretation':'No predictive or reactive advantage can be established. In this low-rate capture, HFBTP correctly rejected extrapolation below its 10 tx/s lower bound; this is safe abstention, not evidence of predictive underperformance or benefit.'
    }
    dump(out/'experiment_summary.json',summary)

    rels=[
      'docs/research/e1/phase2_paired_controller_experimental_protocol_2026-11-11.md',
      'scripts/controllers/run_integrated_controller.py','scripts/controllers/analyze_phase3_shadow_comparison.py',
      'src/fabric_perf/controllers/live_integration.py','src/fabric_perf/controllers/strategies.py',
      'src/fabric_perf/controllers/adapters.py','src/fabric_perf/controllers/harness.py',
      'docs/research/e1/phase2a3_contemporaneous_transaction_evidence_20261111.md',
      'docs/research/e1/phase2b1r8_final_independent_actuator_safety_audit_2026-11-11.md',
      'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/evidence_summary.json',
      'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/controller_frames.jsonl',
      'experiments/artifacts/hfbtp/metadata.json','experiments/artifacts/hfbtp/throughput.joblib',
      'experiments/artifacts/hfbtp/avg_latency.joblib','data/processed/flashbots/series_2026-09-21_1s.csv.gz',
      'tests/test_controllers.py','tests/test_live_controller_integration.py',
      'tests/test_flashbots_forecasting.py','tests/test_model_artifacts.py',
    ]
    sources={}
    for rel in rels:
        p=root/rel
        sources[rel]={'sha256':sha256(p),'size_bytes':p.stat().st_size}
    outputs={p.relative_to(out).as_posix():{'sha256':sha256(p),'size_bytes':p.stat().st_size} for p in out.rglob('*') if p.is_file() and p.name!='reproducibility_manifest.json'}
    dump(out/'reproducibility_manifest.json',{
      'run_id':manifest['run_id'],'executed_at_utc':decisions[0]['integration']['decision_start_timestamp_utc'],
      'command':'python scripts/controllers/run_integrated_controller.py --run-id phase3-shadow-c001 --snapshots experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/controller_frames.jsonl --out-dir experiments/results/e1/phase3_three_controller_comparison_20261111/controller_replay --hfbtp-artifact-dir experiments/artifacts/hfbtp --flashbots-series data/processed/flashbots/series_2026-09-21_1s.csv.gz --flashbots-baseline ewma_alpha_0.2 --horizon-seconds 10 --candidate-batch-sizes 40,45 --hfbtp-block-map 40:40,45:45 --batch-size 40 --batch-timeout-ms 2000 --preferred-max-bytes 10485760 --latency-high-ms 1500 --latency-low-ms 1200 --max-telemetry-age-s 5',
      'python_version':'3.14.3','pytest_version':'9.1.1','mode':'shadow','live_submission_enabled':False,
      'protocol_deviation_file':'pre_run_readiness_and_deviation.md','source_sha256':sources,'output_sha256':outputs,
      'controller_run_manifest':manifest,'observed_performance_source':'Phase 2A.3 contemporaneous evidence; not Phase 3 arm-specific',
      'limitations':['one trace and one observed Fabric capture','two temporally dependent eligible frames','no strategy applied to Fabric','HFBTP out-of-domain at observed arrival rate','no timestamp-matched Flashbots source observations for the November Fabric capture']
    })

if __name__=='__main__': main()




