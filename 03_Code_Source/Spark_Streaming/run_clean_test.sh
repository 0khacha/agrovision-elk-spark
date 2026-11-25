#!/usr/bin/env bash
# AgroVision - execution de reference du flux SANS arret (results/run1_sans_arret) :
# Spark Streaming + generateur, attente de fin, verification et export.
H=$HOME/tp6_elk; S=$H/stream; OUT=$H/results/run1_sans_arret
mkdir -p $OUT $H/logs
rm -rf $S/{incoming,rejected,output,checkpoint,tmp}
mkdir -p $S/{incoming,rejected,output,checkpoint,tmp}
echo "== Run sans arret : Spark, puis generateur (2 s entre fichiers)"
spark-submit --driver-memory 1g $H/src/streaming/agro_stream.py > $H/logs/stream_run1.log 2>&1 &
sleep 30
python3 $H/scripts/generate_stream.py --interval 2 > $H/logs/generator_run1.log 2>&1
echo "== Generateur termine, attente que les 3 requetes aient traite le dernier fichier"
bash $H/scripts/wait_stream_idle.sh
pkill -f agro_stream.py
sleep 5
echo "== Verification"
spark-submit $H/src/streaming/verify_stream.py 2>&1 | grep -v WARN | tee $OUT/verification.txt
echo "== Export"
spark-submit $H/src/streaming/export_run.py $OUT 2>&1 | grep -E "EXPORT_OK|Error|Exception"
cp $H/results/stream_manifest.json $OUT/
