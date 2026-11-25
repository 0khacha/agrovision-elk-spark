#!/usr/bin/env bash
# Attend que les 3 requetes aient traite le DERNIER fichier de incoming/ (2 controles consecutifs)
H=$HOME/tp6_elk; C=$H/stream/checkpoint
last=$(ls $H/stream/incoming | sort | tail -1)
n() { ls "$1" 2>/dev/null | grep -vc '\.crc$'; }
good=0; sig=""
for i in $(seq 1 120); do
  ok=1; sig=""
  for q in events rejected metrics; do
    o=$(n $C/$q/offsets); c=$(n $C/$q/commits)
    [ "$o" != "$c" ] && ok=0
    grep -q "$last" $C/$q/sources/0/* 2>/dev/null || ok=0
    sig="$sig $q:$o/$c"
  done
  if [ $ok = 1 ]; then good=$((good+1)); else good=0; fi
  if [ $good -ge 2 ]; then echo "requetes a jour, dernier fichier $last traite :$sig"; exit 0; fi
  sleep 5
done
echo "TIMEOUT : $last non traite par toutes les requetes :$sig"; exit 1
