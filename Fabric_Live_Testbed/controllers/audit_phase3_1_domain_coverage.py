"""Read-only Phase 3.1 audit of Flashbots domain coverage and prior replay decisions."""
from __future__ import annotations
import argparse, csv, gzip, hashlib, json, math, re, statistics
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
LOW, HIGH=10.0, 200.0
FREQS=(1,5,10)
DAYS=('2026-09-21','2026-10-12')

def sha256(path:Path)->str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def read_jsonl(path):
    return [json.loads(x) for x in Path(path).read_text(encoding='utf-8-sig').splitlines() if x.strip()]

def iso(s): return datetime.fromisoformat(str(s).replace('Z','+00:00')).astimezone(timezone.utc)

def parse_thresholds(path:Path):
    rows=list(csv.DictReader(path.open(encoding='utf-8-sig',newline='')))
    # Use the registered cross-day thresholds (September development only)
    out={}
    for freq in FREQS:
        vals={float(r['burst_threshold']) for r in rows if r['mode']=='cross_day' and r['date_utc']=='2026-10-12' and int(r['frequency_seconds'])==freq}
        if len(vals)!=1: raise ValueError(f'expected one registered cross-day burst threshold at {freq}s, found {vals}')
        out[freq]=next(iter(vals))
    return out,rows

def run_stats(path:Path, expected_day:str, freq:int, burst_threshold_count:float):
    total=below=inside=above=burst=0; sum_rate=0.0; min_rate=float('inf'); max_rate=float('-inf')
    zero=0; transitions=Counter(); episodes={k:[] for k in ('rising','falling','stable')}; cur_kind=None; cur_len=0
    burst_episodes=[]; burst_len=0; burst_windows=0; previous=None; first_ts=last_ts=None; schema=None
    context_bins=max(60//freq,1); history=deque(maxlen=context_bins)
    with gzip.open(path,'rt',encoding='utf-8',newline='') as f:
        reader=csv.DictReader(f)
        schema=reader.fieldnames
        required={'timestamp_utc','arrival_count','frequency_seconds','date_utc'}
        if not required.issubset(set(schema or ())): raise ValueError(f'{path} missing columns {required-set(schema or ())}')
        for row in reader:
            date=row['date_utc']; period=int(row['frequency_seconds']); count=int(row['arrival_count'])
            if date!=expected_day or period!=freq: raise ValueError(f'Unexpected date/frequency in {path}: {date}/{period}')
            ts=iso(row['timestamp_utc'])
            if last_ts and ts<=last_ts: raise ValueError(f'Non-monotonic timestamps in {path}')
            if first_ts is None:first_ts=ts
            last_ts=ts
            rate=count/freq; total+=1; sum_rate+=rate; min_rate=min(min_rate,rate); max_rate=max(max_rate,rate)
            if count==0:zero+=1
            if rate<LOW:below+=1
            elif rate<=HIGH:inside+=1
            else:above+=1
            if len(history)==context_bins:
                burst_windows+=1
                context_mean=sum(history)/context_bins
                is_burst=context_mean>=burst_threshold_count
                if is_burst:
                    burst+=1; burst_len+=1
                elif burst_len:
                    burst_episodes.append(burst_len*freq); burst_len=0
            history.append(count)
            if previous is not None:
                sign='stable' if rate==previous else ('rising' if rate>previous else 'falling')
                transitions[sign]+=1
                if cur_kind==sign:cur_len+=1
                else:
                    if cur_kind is not None:episodes[cur_kind].append(cur_len*freq)
                    cur_kind=sign;cur_len=1
            previous=rate
    if cur_kind is not None:episodes[cur_kind].append(cur_len*freq)
    if burst_len:burst_episodes.append(burst_len*freq)
    duration=total*freq
    if duration!=86400: raise ValueError(f'processed series does not cover a complete UTC day: {path} {duration}s')
    reg={k:{'adjacent_intervals':int(transitions[k]),'total_duration_seconds':int(transitions[k]*freq),
            'contiguous_episodes':len(episodes[k]),'longest_episode_seconds':max(episodes[k],default=0)} for k in episodes}
    return {'source_path':str(path.relative_to(ROOT)).replace('\\','/'),'date_utc':expected_day,'frequency_seconds':freq,
      'bins':total,'covered_interval_seconds':duration,'first_timestamp_utc':first_ts.isoformat(),'last_timestamp_utc':last_ts.isoformat(),
      'zero_arrival_bins':zero,'arrival_rate_tps':{'minimum':min_rate,'maximum':max_rate,'mean':sum_rate/total,
      'below_10_tps_bins':below,'below_10_tps_percent':100*below/total,
      'within_10_to_200_tps_bins':inside,'within_10_to_200_tps_percent':100*inside/total,
      'above_200_tps_bins':above,'above_200_tps_percent':100*above/total},
      'burst_threshold_count_per_bin':burst_threshold_count,'burst_threshold_equivalent_tps':burst_threshold_count/freq,
      'burst_context_window_seconds':60,'burst_context_rule':'mean arrival count in the preceding 60 seconds, excluding the current bin; early bins without complete context are not classified',
      'burst_classified_windows':burst_windows,'burst_windows':burst,'burst_percent':100*burst/burst_windows if burst_windows else None,
      'burst_contiguous_episodes':len(burst_episodes),'burst_duration_seconds':sum(burst_episodes),
      'burst_longest_episode_seconds':max(burst_episodes,default=0),
      'directional_change_descriptors':reg,'direction_definition':'strict adjacent-bin rate change; equality is stable; no extra tolerance or threshold',
      'schema':schema}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--out-dir',type=Path,required=True); ap.add_argument('--refresh-existing',action='store_true',help='refresh generated files in an existing Phase 3.1 output directory'); a=ap.parse_args(); out=a.out_dir.resolve()
    if out.exists() and not a.refresh_existing:raise SystemExit(f'refusing to overwrite {out}')
    out.mkdir(parents=True,exist_ok=True)
    data=ROOT/'data'; res=ROOT/'experiments/results'; e1=res/'e1'
    metrics_path=res/'flashbots_baseline_metrics.csv'; thresholds,metric_rows=parse_thresholds(metrics_path)
    # Archive hashes are checked against the previous independent archive audit and current registered manifest.
    archive_audit=json.loads((res/'flashbots_archive_audit.json').read_text(encoding='utf-8-sig'))
    manifest_text=(data/'manifests/flashbots_mempool_manifest.yaml').read_text(encoding='utf-8-sig')
    archives=[]
    for entry in archive_audit['archives']:
        p=Path(entry['archive']); actual=sha256(p); expected=entry['sha256'].lower()
        if actual!=expected or expected.upper() not in manifest_text.upper():raise ValueError(f'archive checksum does not match audit/manifest: {p}')
        archives.append({'path':str(p),'csv_entry':entry['csv_entry'],'rows':entry['rows'],'sha256':actual,
          'archive_crc_valid':entry['archive_crc_valid'],'checksum_matches_audit_and_manifest':True})
    # Model limits are read from existing code and artifact metadata; no limits are changed.
    adapter_text=(ROOT/'src/fabric_perf/controllers/adapters.py').read_text(encoding='utf-8')
    m=re.search(r'ARRIVAL_MIN,\s*ARRIVAL_MAX\s*=\s*([0-9.]+),\s*([0-9.]+)',adapter_text)
    if not m:raise ValueError('could not locate existing HFBTP arrival domain')
    domain={'arrival_tps_min_inclusive':float(m.group(1)),'arrival_tps_max_inclusive':float(m.group(2))}
    hfmeta=json.loads((ROOT/'experiments/artifacts/hfbtp/metadata.json').read_text(encoding='utf-8-sig'))
    # Audit all six separate daily/frequency series using streaming gzip CSV reads.
    series=[]
    for day in DAYS:
      for freq in FREQS:
        p=data/'processed/flashbots'/f'series_{day}_{freq}s.csv.gz'
        series.append(run_stats(p,day,freq,thresholds[freq]))
    # Existing forecast benchmark origin counts: one deterministic baseline row per mode/regime/horizon.
    origin_groups={}
    for r in metric_rows:
        if r['baseline']!='persistence':continue
        key=(r['mode'],r['date_utc'],int(r['frequency_seconds']),int(r['horizon_seconds']))
        origin_groups.setdefault(key,{})[r['regime']]=int(r['forecast_origins'])
    origin_summaries=[{'mode':k[0],'date_utc':k[1],'frequency_seconds':k[2],'horizon_seconds':k[3],
      'origins_by_regime':v,'total_reported_origins':sum(v.values()),
      'qualification':'existing baseline benchmark counts, not Fabric telemetry frames'} for k,v in sorted(origin_groups.items())]

    run_id='shadow-replay-cfg001-20261111-005'
    run=e1/'synchronized_shadow_replay'/run_id
    snaps=read_jsonl(run/'controller_snapshots.jsonl')
    shadow=run/'controller_shadow_runs'
    records={n:json.loads((shadow/f'{n}.json').read_text(encoding='utf-8-sig')) for n in ('static','reactive','predictive')}
    shadow_manifest=json.loads((shadow/'shadow_manifest.json').read_text(encoding='utf-8-sig'))
    if shadow_manifest.get('mode')!='shadow' or shadow_manifest.get('live_submission') is not False or shadow_manifest.get('hfbtp_block_size_mapping')!={'40':40,'45':45}: raise ValueError('Phase 1C shadow manifest does not match approved shadow/candidate mapping')
    if not all(len(records[n])==len(snaps)==20 for n in records):raise ValueError('unexpected Phase 1C frame/decision counts')
    phase1c=json.loads((run/'synchronized_shadow_replay_evidence.json').read_text(encoding='utf-8-sig'))
    reactive_high=1500.0; reactive_low=1200.0
    burst1=thresholds[1]
    eligible=[]; excluded=[]; normalized=[]; comparisons=[]; counterfactual=[]
    observed_latency_high=[]; capture_times=[]
    for i,snap in enumerate(snaps):
      src_ts=snap['timestamp_utc']; cap_ts=snap.get('capture_timestamp_utc'); capture_times.append(iso(cap_ts) if cap_ts else None)
      p=records['predictive'][i]; state=p['state']; dec=p['decision']; md=dec.get('metadata',{})
      forecast=md.get('forecast',{}); candidates=md.get('candidate_utilities',[])
      history_ts=state.get('arrival_history_timestamps_utc',[]); history=state.get('arrival_history_tps',[])
      config=state.get('current_config') or {}; pred=dec.get('predicted_arrival_rate_tps')
      reasons=[]
      if not history or len(history)!=len(history_ts):reasons.append('missing_or_misaligned_forecast_history')
      if history_ts and (history_ts!=sorted(history_ts) or iso(history_ts[-1])>iso(src_ts)):reasons.append('forecast_history_not_chronological_or_contains_future_timestamp')
      if history_ts and iso(history_ts[-1])!=iso(src_ts):reasons.append('forecast_history_does_not_end_at_decision_origin')
      if snap.get('provenance',{}).get('future_data_excluded') is not True:reasons.append('source_provenance_does_not_confirm_future_data_exclusion')
      if snap.get('trace_bin',{}).get('timestamp_utc') and iso(snap['trace_bin']['timestamp_utc'])!=iso(src_ts):reasons.append('trace_bin_timestamp_mismatch')
      if forecast.get('origin_timestamp_utc') and iso(forecast['origin_timestamp_utc'])!=iso(src_ts):reasons.append('forecast_origin_does_not_match_decision_origin')
      elif not forecast.get('origin_timestamp_utc'):reasons.append('forecast_origin_timestamp_missing')
      if state.get('telemetry_age_s') is None or float(state['telemetry_age_s'])>5:reasons.append('stale_or_unavailable_telemetry_age')
      if pred is None or not(domain['arrival_tps_min_inclusive']<=float(pred)<=domain['arrival_tps_max_inclusive']):reasons.append('forecast_arrival_outside_hfbtp_domain')
      if int(config.get('orderers') or 0) not in range(3,10):reasons.append('orderer_count_outside_hfbtp_domain_or_missing')
      if int(config.get('max_message_count') or 0) not in (40,45):reasons.append('current_configuration_not_in_candidate_space')
      if len(candidates)!=2 or {int(x['config']['max_message_count']) for x in candidates}!={40,45}:reasons.append('candidate_mapping_or_predictions_incomplete')
      if any(x.get('metadata',{}).get('error') for x in candidates):reasons.append('candidate_prediction_error')
      # A missing latency at a sample is recorded but is not an HFBTP feature; it is important to reactive eligibility.
      if state.get('observed_latency_ms') is None: reasons.append('reactive_controller_latency_observation_missing')
      # The controller run had measured some but not all latency samples; predictive domain eligibility is independent.
      hard=[x for x in reasons if x!='reactive_controller_latency_observation_missing']
      item={'source_run_id':run_id,'frame_index':i,'trace_timestamp_utc':src_ts,'capture_timestamp_utc':cap_ts,
        'source_date_utc':snap.get('trace_bin',{}).get('date_utc'),'frequency_seconds':snap.get('trace_bin',{}).get('frequency_seconds'),
        'source_bin':snap.get('trace_bin',{}).get('source_bin'),'observed_arrival_tps':state.get('observed_arrival_rate_tps'),
        'predicted_arrival_tps':pred,'forecast_model':forecast.get('model_identity'),'forecast_horizon_seconds':forecast.get('horizon_seconds'),
        'forecast_origin_timestamp_utc':forecast.get('origin_timestamp_utc'),'forecast_source':forecast.get('source'),
        'forecast_inference_ms':forecast.get('inference_duration_ms'),'telemetry_age_s':state.get('telemetry_age_s'),
        'history_length':len(history),'history_last_timestamp_utc':history_ts[-1] if history_ts else None,
        'batch_size':config.get('max_message_count'),'batch_timeout_ms':config.get('batch_timeout_ms'),
        'preferred_max_bytes':config.get('preferred_max_bytes'),'absolute_max_bytes':config.get('absolute_max_bytes'),
        'orderers':config.get('orderers'),'candidate_max_message_counts':'40,45',
        'hfbptp_domain_min_tps':domain['arrival_tps_min_inclusive'],'hfbptp_domain_max_tps':domain['arrival_tps_max_inclusive'],
        'burst_threshold_tps_sept_development':burst1,'origin_bin_intensity_label':'above_development_rate_threshold' if float(state.get('observed_arrival_rate_tps') or 0)>=burst1 else 'at_or_below_development_rate_threshold',
        'predictive_decision':dec.get('reason'),'predictive_recommendation':dec.get('config',{}).get('max_message_count'),
        'predictive_changed':dec.get('changed'),'eligibility':'ELIGIBLE' if not hard else 'EXCLUDED',
        'reactive_latency_observation_missing':state.get('observed_latency_ms') is None,
        'eligibility_notes':';'.join(reasons) if reasons else 'all existing HFBTP, forecast provenance, history and telemetry-age checks pass'}
      (eligible if not hard else excluded).append(item)
      if state.get('observed_latency_ms') is None:
        excluded.append({'source_run_id':run_id,'frame_index':i,'trace_timestamp_utc':src_ts,'capture_timestamp_utc':cap_ts,
          'eligibility':'EXCLUDED_FROM_REACTIVE_LATENCY_TRIGGER_ANALYSIS_ONLY',
          'eligibility_notes':'observed_latency_ms is null in source frame; this row remains included in the 20-frame Predictive-domain and shared-input decision audit'})
      for strategy in ('static','reactive','predictive'):
        rec=records[strategy][i]
        # Verify decision state is exactly the same across controllers for this frame.
        if rec['state']!=state:raise ValueError(f'controller input mismatch at frame {i}: {strategy}')
        normalized.append({'source_run_id':run_id,'frame_index':i,'trace_timestamp_utc':src_ts,
          'capture_timestamp_utc':cap_ts,'strategy':strategy,'record':rec,
          'evidence_source':'existing Phase 1C controller shadow record; not rerun in Phase 3.1'})
      stat=records['static'][i]['decision']; react=records['reactive'][i]['decision']; pred_dec=records['predictive'][i]['decision']
      latency=state.get('observed_latency_ms')
      if latency is not None:observed_latency_high.append(float(latency)>reactive_high)
      comparisons.append({'source_run_id':run_id,'frame_index':i,'trace_timestamp_utc':src_ts,'capture_timestamp_utc':cap_ts,
        'input_arrival_tps':state.get('observed_arrival_rate_tps'),'input_bin_intensity_label':item['origin_bin_intensity_label'],
        'shared_observed_throughput_tps':state.get('observed_throughput_tps'),'shared_observed_latency_ms':latency,
        'shared_queue_depth':state.get('queue_depth'),'static_reason':stat.get('reason'),'static_recommendation':stat.get('config',{}).get('max_message_count'),
        'reactive_reason':react.get('reason'),'reactive_recommendation':react.get('config',{}).get('max_message_count'),
        'reactive_congestion_streak':react.get('metadata',{}).get('congestion_streak'),
        'predictive_reason':pred_dec.get('reason'),'predictive_recommendation':pred_dec.get('config',{}).get('max_message_count'),
        'predictive_forecast_tps':pred_dec.get('predicted_arrival_rate_tps'),'predictive_changed':pred_dec.get('changed'),
        'predictive_utility_improvement':pred_dec.get('metadata',{}).get('utility_improvement'),
        'same_inputs_across_controllers':True,'actuator_submitted':False})
      for cand in md.get('candidate_utilities',[]):
        pc=md.get('performance_predictor',{})
        counterfactual.append({'source_run_id':run_id,'frame_index':i,'trace_timestamp_utc':src_ts,
          'forecast_origin_timestamp_utc':forecast.get('origin_timestamp_utc'),'forecast_arrival_tps':pred,
          'candidate_fabric_max_message_count':cand['config']['max_message_count'],
          'mapped_hfbtp_block_size':40 if cand['config']['max_message_count']==40 else 45,
          'predicted_throughput_tps':cand.get('throughput_tps'),'predicted_latency_ms':cand.get('latency_ms'),
          'predicted_utility':cand.get('utility'),'selected_by_predictive_controller':cand['config']['max_message_count']==pred_dec.get('config',{}).get('max_message_count'),
          'hfbpt_model_identity':pc.get('model_identity'),'forecast_model_identity':forecast.get('model_identity'),
          'domain_check':'PASS' if domain['arrival_tps_min_inclusive']<=float(pred)<=domain['arrival_tps_max_inclusive'] else 'FAIL',
          'interpretation':'offline frozen-model estimate only; not observed Fabric outcome; no model uncertainty interval supplied'})
    # Timestamp spacing between real capture samples is checked independently of historical trace time.
    capture_deltas=[(capture_times[i]-capture_times[i-1]).total_seconds() for i in range(1,len(capture_times)) if capture_times[i] and capture_times[i-1]]
    if capture_times and any(x is None for x in capture_times):raise ValueError('missing capture wall-clock timestamp in Phase 1C snapshots')
    if capture_deltas and any(x<=0 or x>5 for x in capture_deltas):raise ValueError('capture timestamp gap exceeds 5 seconds')
    decision_summary={}
    for strategy,items in records.items():
      vals=[float(x['decision'].get('decision_overhead_ms') or 0.0) for x in items]
      q=statistics.quantiles(vals,n=4,method='inclusive')
      decision_summary[strategy]={'decision_count':len(items),'reason_counts':dict(Counter(x['decision']['reason'] for x in items)),
        'recommended_max_message_count_counts':dict(Counter(str(x['decision']['config']['max_message_count']) for x in items)),
        'proposed_changes':sum(bool(x['decision'].get('changed')) for x in items),
        'decision_latency_ms':{'median':statistics.median(vals),'q1':q[0],'q3':q[2],'iqr':q[2]-q[0],'p95_nearest_rank':sorted(vals)[math.ceil(.95*len(vals))-1]}}
    pair_diffs={}
    for other in ('reactive','static'):
      diffs=[records['predictive'][i]['decision']['config']['max_message_count']-records[other][i]['decision']['config']['max_message_count'] for i in range(len(snaps))]
      pair_diffs['predictive_minus_'+other]={'difference_counts':dict(Counter(str(x) for x in diffs)),
        'median':statistics.median(diffs),'q1':statistics.quantiles(diffs,n=4,method='inclusive')[0],
        'q3':statistics.quantiles(diffs,n=4,method='inclusive')[2],'iqr':statistics.quantiles(diffs,n=4,method='inclusive')[2]-statistics.quantiles(diffs,n=4,method='inclusive')[0],
        'inference':'descriptive only; one temporally dependent trace'}
    # Phase 2A.3 frames provide explicit out-of-domain and critical-missing exclusions.
    p2dir=e1/'phase2a3_contemporaneous_evidence_20261111'
    p2all=read_jsonl(p2dir/'controller_frames_all.jsonl')
    p2valid=read_jsonl(p2dir/'controller_frames.jsonl')
    for idx,row in enumerate(p2all):
      state=row.get('state',row); missing=row.get('critical_missing',[])
      rate=state.get('observed_arrival_rate_tps')
      if missing:
        reason='critical_missing_fields:'+','.join(missing)
      elif rate is None or not(domain['arrival_tps_min_inclusive']<=float(rate)<=domain['arrival_tps_max_inclusive']):
        reason=f'observed/forecast input rate {rate} TPS below HFBTP minimum {domain["arrival_tps_min_inclusive"]} TPS; phase3 predictive adapter recorded domain rejection'
      else:continue
      excluded.append({'source_run_id':'phase2a3-c001','frame_index':idx,'trace_timestamp_utc':state.get('timestamp_utc'),
        'capture_timestamp_utc':row.get('observation_timestamp_utc'),'source_date_utc':'2026-11-10',
        'observed_arrival_tps':rate,'predicted_arrival_tps':None,'hfbpt_domain_min_tps':domain['arrival_tps_min_inclusive'],
        'hfbpt_domain_max_tps':domain['arrival_tps_max_inclusive'],'eligibility':'EXCLUDED','eligibility_notes':reason,
        'source_missing_fields':';'.join(missing)})
    # JSONL/CSV helpers
    def write_jsonl(path,rows):
      with path.open('w',encoding='utf-8',newline='') as f:
       for x in rows:f.write(json.dumps(x,sort_keys=True)+'\n')
    def write_csv(path,rows):
      if not rows:
       path.write_text('',encoding='utf-8');return
      with path.open('w',encoding='utf-8-sig',newline='') as f:
       w=csv.DictWriter(f,fieldnames=list(dict.fromkeys(k for x in rows for k in x)));w.writeheader();w.writerows(rows)
    write_csv(out/'eligible_frames.csv',eligible);write_csv(out/'excluded_frames.csv',excluded)
    write_jsonl(out/'controller_decisions.jsonl',normalized);write_csv(out/'decision_comparison.csv',comparisons)
    write_csv(out/'counterfactual_estimates.csv',counterfactual)
    # Original archive and processed-series digests for reproducibility.
    files=[data/'manifests/flashbots_mempool_manifest.yaml',res/'flashbots_archive_audit.json',data/'processed/flashbots/data_quality_audit.json',
      metrics_path,res/'flashbots_baseline_metrics.metadata.json',ROOT/'experiments/artifacts/hfbtp/metadata.json',
      ROOT/'src/fabric_perf/controllers/adapters.py',ROOT/'src/fabric_perf/controllers/strategies.py',
      ROOT/'scripts/controllers/run_integrated_controller.py',ROOT/'docs/research/e1/phase2_paired_controller_experimental_protocol_2026-11-11.md',
      ROOT/'docs/research/e1/flashbots_fabric_synchronized_shadow_replay_2026-11-11.md',
      ROOT/'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/controller_frames_all.jsonl',
      ROOT/'experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/controller_frames.jsonl',
      ROOT/'experiments/results/e1/phase2a3_controller_shadow_20261111_v2/controller_decisions.jsonl',
      shadow/'shadow_manifest.json',ROOT/'experiments/artifacts/hfbtp/throughput.joblib',ROOT/'experiments/artifacts/hfbtp/avg_latency.joblib',
      *[Path(x['path']) for x in archives],
      *[data/'processed/flashbots'/f'series_{d}_{f}s.csv.gz' for d in DAYS for f in FREQS],
      ROOT/'scripts/controllers/audit_phase3_1_domain_coverage.py',run/'replay_manifest.json',run/'controller_snapshots.jsonl',
      run/'forecast_evidence.json',run/'synchronized_shadow_replay_evidence.json',shadow/'static.json',shadow/'reactive.json',shadow/'predictive.json']
    def input_key(p):
      try:return str(p.relative_to(ROOT)).replace('\\','/')
      except ValueError:return str(p).replace('\\','/')
    file_hashes={input_key(p):sha256(p) for p in files}
    coverage={'audit_status':'READ_ONLY_AUDIT_COMPLETE','hfbpt_domain':domain,'hfbpt_metadata':hfmeta,
      'raw_archives':archives,'dates_kept_separate':True,'no_gap_interpolation':True,
      'registered_burst_threshold_source':'cross-day forecast baseline metadata; derived from Sep 21 development split only',
      'burst_thresholds_tps_by_frequency':{str(k):v/k for k,v in thresholds.items()},
      'daily_series_coverage':series,'existing_forecast_benchmark_origins':origin_summaries,
      'existing_domain_valid_replay':{'run_id':run_id,'trace':'2026-09-21 first 20 one-second bins',
        'decision_behavior_summary':decision_summary,'paired_recommendation_differences':pair_diffs,
        'forecast_metrics_from_existing_evidence':phase1c['forecast_evidence'],
        'frames':len(snaps),'forecast_available':len(snaps),'forecast_horizon_seconds':10,
        'forecast_arrival_domain_pass':sum(domain['arrival_tps_min_inclusive']<=float(r['decision']['predicted_arrival_rate_tps'])<=domain['arrival_tps_max_inclusive'] for r in records['predictive']),
        'forecast_min_tps':min(float(r['decision']['predicted_arrival_rate_tps']) for r in records['predictive']),
        'forecast_max_tps':max(float(r['decision']['predicted_arrival_rate_tps']) for r in records['predictive']),
        'controller_decisions_per_strategy':{k:len(v) for k,v in records.items()},
        'timestamped_capture_interval_seconds':{'min':min(capture_deltas),'max':max(capture_deltas),'median':statistics.median(capture_deltas)},
        'telemetry_age_s_values':sorted({float(r['state']['telemetry_age_s']) for r in records['predictive']}),
        'history_length_min':min(len(r['state']['arrival_history_tps']) for r in records['predictive']),
        'history_length_max':max(len(r['state']['arrival_history_tps']) for r in records['predictive']),
        'predictive_actions':sum(bool(r['decision']['changed']) for r in records['predictive']),
        'predictive_reason_counts':dict(Counter(r['decision']['reason'] for r in records['predictive'])),
        'reactive_reason_counts':dict(Counter(r['decision']['reason'] for r in records['reactive'])),
        'static_reason_counts':dict(Counter(r['decision']['reason'] for r in records['static'])),
        'latency_samples_available':sum(r['state'].get('observed_latency_ms') is not None for r in records['predictive']),
        'latency_samples_missing':sum(r['state'].get('observed_latency_ms') is None for r in records['predictive']),
        'latency_above_reactive_high_threshold':sum(float(r['state']['observed_latency_ms'])>reactive_high for r in records['predictive'] if r['state'].get('observed_latency_ms') is not None),
        'reactive_high_latency_threshold_ms':reactive_high,'reactive_recovery_threshold_ms':reactive_low,
        'queue_high_threshold_configured':False,'capture_source_clock_separate_from_trace_clock':True,
        'source_date_series_is_not_joined_to_october':True},
      'controller_frame_eligibility':{'phase1c_candidate_frames':len(snaps),'eligible_predictive_frames':len(eligible),'excluded_from_predictive_domain_frames':sum(x.get('source_run_id')=='phase2a3-c001' for x in excluded),
        'excluded_from_reactive_latency_analysis_frames':sum(x.get('eligibility')=='EXCLUDED_FROM_REACTIVE_LATENCY_TRIGGER_ANALYSIS_ONLY' for x in excluded),
        'complete_reactive_latency_frames':sum(r['state'].get('observed_latency_ms') is not None for r in records['predictive']),
        'eligibility_rules':'forecast arrival in [10,200] TPS; supported B-to-HFBTP map for 40/45; orderers 3; forecast origin matches trace origin; chronological history with no future timestamp; telemetry_age_s <=5; both candidate predictions present',
        'excluded_rows_include_phase2a3_low_rate_and_missing_telemetry':True},
      'workload_regime_scope':{'burst':'registered September development threshold applied to both days at all frequencies',
        'rising_falling_stable':'strict adjacent-bin direction descriptors, not controller regimes; exact equality defines stable; no additional threshold added',
        'congestion_like':'cannot be estimated for daily Flashbots series without Fabric telemetry; Phase1C replay latency assessed against existing ReactiveController 1500ms high threshold; no queue threshold is configured'},
      'repetition_feasibility':{'independent_flashbots_days':2,'domain_valid_fabric_replay_runs':1,'paired_repetitions_per_strategy_regime':1,
        'protocol_minimum_paired_repetitions_per_strategy_regime':5,'sufficient_for_preregistered_comparison':False,
        'reason':'Only one existing 20-second Phase1C synchronized Fabric replay; its adjacent frames are dependent, and October has no aligned Fabric capture.'},
      'file_sha256':file_hashes}
    (out/'domain_coverage.json').write_text(json.dumps(coverage,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    # Reproducibility manifest hashes all original inputs and generated outputs; manifest excludes itself.
    outputs={str(p.relative_to(out)).replace('\\','/'):sha256(p) for p in out.rglob('*') if p.is_file() and p.name!='reproducibility_manifest.json'}
    manifest={'analysis':'Phase 3.1 read-only domain coverage and existing shadow decision audit','new_controller_execution':False,
      'live_actuation':False,'raw_dataset_modified':False,'phase3_evidence_modified':False,
      'script':'scripts/controllers/audit_phase3_1_domain_coverage.py','python_version':__import__('sys').version,
      'input_sha256':file_hashes,'output_sha256':outputs,
      'existing_replay_run_id':run_id,'protocol_sha256':sha256(ROOT/'docs/research/e1/phase2_paired_controller_experimental_protocol_2026-11-11.md')}
    tests={'validation':'PASS','method':'audit-script assertions and machine-evidence consistency checks','new_live_or_model_tests':False,
      'checks':{'archive_hashes_match_prior_audit_and_manifest':True,'all_six_daily_series_read_and_chronology_checked':True,
        'phase1c_strategy_inputs_identical':True,'phase1c_forecast_timestamps_and_history_checked':True,
        'phase1c_hfbtp_domain_and_candidate_map_checked':True,'all_recommendations_shadow_only':True,
        'output_json_csv_jsonl_generated':True},'note':'No controller code or models were changed; prior 25 focused tests remain supporting evidence, not rerun for this audit.'}
    (out/'test_results.json').write_text(json.dumps(tests,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    manifest['output_sha256']={str(p.relative_to(out)).replace('\\','/'):sha256(p) for p in out.rglob('*') if p.is_file() and p.name!='reproducibility_manifest.json'}
    (out/'reproducibility_manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps({'out_dir':str(out),'series':len(series),'phase1c_frames':len(snaps),'eligible':len(eligible),'excluded':len(excluded),
      'forecasts_in_domain':coverage['existing_domain_valid_replay']['forecast_arrival_domain_pass'],'repetitions_sufficient':False,'live_actuation':False},indent=2))
if __name__=='__main__':main()

