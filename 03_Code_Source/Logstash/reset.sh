#!/bin/bash
# Remet tout a zero : Logstash, file d'attente, sincedb, index. Options passees a create_indices.py
pkill -f logstash; sleep 5
pgrep -af logstash && { echo "Logstash tourne encore"; exit 1; }
rm -rf ~/tp3_elk/state/ls_data ~/tp3_elk/state/sincedb_* ~/tp3_elk/state/completed_* ~/tp3_elk/state/settings_*
python3 ~/tp3_elk/scripts/create_indices.py --reset "$@"
sleep 15
curl -s "http://node-1:9200/_cat/indices/agro-load*?v&h=index,health,pri,rep,docs.count"
