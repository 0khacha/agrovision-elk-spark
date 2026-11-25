#!/bin/bash
# AgroVision - bilan qualite : nombre de valides et de rejets, repartition par categorie.
ES=http://node-1:9200
V=$(curl -s $ES/agro-load-v1/_count | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
R=$(curl -s $ES/agro-load-rejected-v1/_count | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])")
G=$(python3 -c "import json;print(json.load(open('$HOME/tp3_elk/data/generation_report.json'))['data_lines'])")
echo "Lignes generees : $G"
echo "Valides indexes : $V"
echo "Rejetes         : $R"
echo "Perdus          : $((G - V - R))"
echo "Doublons docs valides (ids distincts) :"
curl -s $ES/agro-load-v1/_search?size=0 -H 'Content-Type: application/json' -d '{"aggs":{"u":{"cardinality":{"field":"observation_id"}}}}' | grep -E '"value"'
echo "Health :"; curl -s $ES/_cluster/health | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['status'],d['number_of_nodes'],'nodes')"
