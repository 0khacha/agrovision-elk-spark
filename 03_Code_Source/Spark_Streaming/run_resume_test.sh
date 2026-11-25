#!/usr/bin/env bash
# AgroVision - execution AVEC arret brutal de Spark en plein lot puis reprise sur le meme
# checkpoint (results/run2_reprise) ; ecrit kill_evidence.txt et compare au run de reference.
H=$HOME/tp6_elk; S=$H/stream; OUT=$H/results/run2_reprise; C=$S/checkpoint/events
n() { ls "$1" 2>/dev/null | grep -vc '\.crc$'; }
mkdir -p $OUT $H/logs
rm -rf $S/{incoming,rejected,output,checkpoint,tmp}
mkdir -p $S/{incoming,rejected,output,checkpoint,tmp}
echo "== Phase 1 : demarrage de Spark, puis du generateur (3 s entre fichiers)"
spark-submit --driver-memory 1g $H/src/streaming/agro_stream.py > $H/logs/stream_run2_part1.log 2>&1 &
sleep 30
python3 $H/scripts/generate_stream.py --interval 3 > $H/logs/generator_run2.log 2>&1 &
GEN=$!
while [ "$(ls $S/incoming | wc -l)" -lt 20 ]; do sleep 1; done
echo "== Attente d'un lot en cours (offsets > commits), 90 s max"
INFLIGHT=non
for i in $(seq 1 180); do
  o=$(n $C/offsets); c=$(n $C/commits)
  if [ "$o" -gt "$c" ]; then INFLIGHT=oui; break; fi
  sleep 0.5
done
echo "== KILL -9 de Spark (arret brutal)"
{
  echo "date_kill=$(date -Is)"
  echo "lot_en_cours_au_kill=$INFLIGHT"
  echo "fichiers_incoming_au_kill=$(ls $S/incoming | wc -l)"
  echo "lots_offsets_events=$(n $C/offsets)"
  echo "lots_commits_events=$(n $C/commits)"
  echo "fichiers_parquet_events_au_kill=$(ls $S/output/events/*.parquet 2>/dev/null | wc -l)"
} | tee $OUT/kill_evidence.txt
pkill -9 -f agro_stream.py
sleep 15
echo "== Phase 2 : redemarrage avec les MEMES checkpoints (le generateur continue)"
spark-submit --driver-memory 1g $H/src/streaming/agro_stream.py > $H/logs/stream_run2_part2.log 2>&1 &
wait $GEN
echo "== Generateur termine, attente que les 3 requetes aient traite le dernier fichier"
bash $H/scripts/wait_stream_idle.sh
pkill -f agro_stream.py
sleep 5
echo "== Verification"
spark-submit $H/src/streaming/verify_stream.py 2>&1 | grep -v WARN | tee $OUT/verification.txt
echo "== Export"
spark-submit $H/src/streaming/export_run.py $OUT 2>&1 | grep -E "EXPORT_OK|Error|Exception"
cp $H/results/stream_manifest.json $OUT/
