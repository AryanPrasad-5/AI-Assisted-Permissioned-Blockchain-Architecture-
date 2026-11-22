#!/usr/bin/env bash
# Read-only watcher: emit decision / transition / failure events of one run; exit when run_result.json exists or launcher exits.
R=$1
RD=/home/mailf18/e1/closed-loop-20261123/$R
LOG=/home/mailf18/e1/closed-loop-20261123/${R}_runner_console.log
seen=0
for i in $(seq 1 1800); do
  if [ -f "$RD/events.jsonl" ]; then
    n=$(wc -l < "$RD/events.jsonl")
    if [ "$n" -gt "$seen" ]; then
      tail -n +"$((seen+1))" "$RD/events.jsonl" | head -n "$((n-seen))" | python3 -c "
import json,sys
for l in sys.stdin:
    try: e=json.loads(l)
    except Exception: continue
    k=e.get('kind'); lab=e.get('label','')
    if k in ('decision','step_failed','telemetry_error') or lab.startswith('actuate') or (k=='command' and e.get('exit_code') not in (0,None)):
        a=e.get('actuation'); a=a if isinstance(a,str) else ({x:a.get(x) for x in ('actuator_exit','duration_ms','error')} if isinstance(a,dict) else a)
        print(e.get('utc','')[11:23], k, lab, 'd=%s'%e.get('decision_index',''), 'cur=%s'%e.get('current_B',''), 'target=%s'%e.get('target_B',''), 'reason=%s'%e.get('reason',''), 'exit=%s'%e.get('exit_code',''), 'act=%s'%a, str(e.get('detail',''))[:200], flush=True)
"
      seen=$n
    fi
  fi
  if [ -f "$RD/run_result.json" ]; then
    python3 -c "import json;r=json.load(open('$RD/run_result.json'));print('RUN_RESULT',r.get('run_id'),'verdict',r.get('verdict'),'step_failure',r.get('step_failure'))"
    exit 0
  fi
  if [ -f "$LOG" ] && grep -q "runner exit code" "$LOG"; then echo "RUNNER EXITED without run_result: $(tail -2 "$LOG")"; exit 0; fi
  sleep 2
done
echo "watcher timeout"
