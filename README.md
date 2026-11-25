# AgroVision – Projet final ELK + Apache Spark

**Intelligence Géospatiale et Temporelle pour la Détection des Transformations Agricoles**
*Des mesures NDVI et capteurs à l'aide à la décision d'irrigation*

Binôme : Abdeljalil Nahimy, Mohamed Khacha. Encadrant : Pr. L. Karbil. ENSIASD Taroudant, 2026-2027.

AgroVision surveille 600 parcelles agricoles (6 régions, 6 cultures, 49 sources d'eau). La plateforme :

- détecte les parcelles en stress hydrique ;
- contrôle la qualité des mesures (NDVI et capteurs) ;
- identifie les sources d'eau critiques.

La chaîne complète passe par Logstash, Elasticsearch, Kibana, Spark Batch, Spark Structured Streaming et GraphX.

---

## 1. Organisation du dossier

```
PROJET_FINAL_ELK_SPARK_Nahimy_Khacha/
├── 01_Rapport/
│   ├── Rapport_Final.pdf
│   └── Annexes_Resultats/ sorties réelles citées dans le rapport : batch/, graphx/, integration/,
│                          run1_sans_arret/, run2_reprise/ (preuve de reprise), logstash_runs.csv
├── 02_Video/              Demonstration_Projet.mp4
├── 03_Code_Source/
│   ├── Data_Generation/   generate_load_data.py (historique), build_graph_data.py (graphe),
│   │                      generate_stream.py (flux), verify_dataset.py
│   ├── Logstash/          agro_pipeline_v2.conf, agro_validate_v2.rb, run_ingest.py,
│   │                      resume_ingest.py, create_indices.py, reset.sh, quality_report.sh
│   ├── Elasticsearch/     mappings et settings des 8 index (*_mapping.json, *_settings.json)
│   ├── Spark_Batch/       batch_analyses.py
│   │   └── Integration/   spark_to_es.py (publication des résultats Spark dans ES),
│   │                      mission7_coherence.py (cohérence historique / flux / graphe)
│   ├── Spark_Streaming/   agro_stream.py, verify_stream.py, verify_stream_demo.py, compare_runs.py,
│   │                      export_run.py, diagnose_missing.py, run_clean_test.sh, run_resume_test.sh,
│   │                      wait_stream_idle*.sh
│   └── GraphX/            agro_graph.scala
├── 04_Datasets/
│   ├── Historical_Data/   history/ : agro_hist.csv (51 000 lignes), anomalies_truth.csv,
│   │                      duplicates_truth.csv, generation_report.json
│   ├── Streaming_Data/    incoming/ (60 fichiers JSONL, 12 360 lignes), stream_manifest.json
│   └── Graph_Data/        graph/ : vertices.csv (755 sommets), edges.csv (2 879 arêtes), graph_report.json
├── 05_Configurations/     elasticsearch_node-{1,2,3}.yml, kibana.yml, kibana_dashboard.ndjson,
│                          logstash.yml, logstash_jvm.options, hosts
└── README.md

Sur vm4, les fichiers sont organisés ainsi :
- `~/tp3_elk/` : logstash/, scripts/ ;
- `~/tp6_elk/` : src/batch, src/streaming, src/graphx, src/integration, scripts/, data/, results/.

Les commandes de la section 4 utilisent ces chemins.
```

## 2. Architecture

| Machine | Rôle | vCPU / RAM |
|---|---|---|
| node-1 | Elasticsearch 8.19 (maître élu + données) | 1 / 1,6 Gio |
| node-2 | Elasticsearch 8.19 (éligible maître + données) | 2 / 1,9 Gio |
| node-3 | Elasticsearch 8.19 (éligible maître + données) | 1 / 1,6 Gio |
| vm4 | Logstash 8.19.23, Kibana 8.19 (port 5601), Spark 3.5.9 (Scala 2.12.18, Java 17) | 4 / 4,8 Gio |

- **Cluster :** `ensiasd-cluster`. La sécurité X-Pack est désactivée (configuration de laboratoire).
- **Adressage :** toutes les configurations utilisent les **noms d'hôtes** (`node-1`, `node-2`, `node-3`, `vm4`), résolus par `/etc/hosts`.
- **Si les adresses IP changent :** mettre à jour `/etc/hosts` sur les 4 machines, puis redémarrer `elasticsearch` et `kibana`.

## 3. Prérequis

- Les 4 machines démarrées, node-1 en premier.
- Un cluster à l'état green :
  ```bash
  curl -s "http://node-1:9200/_cluster/health?pretty"
  ```
- Sur vm4 :
  - Python 3 ;
  - Spark 3.5.9 (`spark-submit` et `spark-shell` dans le PATH) ;
  - Logstash 8.19 dans `/usr/share/logstash`.
- Arborescence de travail sur vm4 :
  - `~/tp3_elk` contient le pipeline Logstash et ses scripts ;
  - `~/tp6_elk` contient Spark, les données et les résultats.

## 4. Reproduction, étape par étape (sur vm4)

### 4.1 Générer les données (reproductible, graine 42)

```bash
# Historique : produit un fichier identique à agro_hist.csv
# (SHA-256 ce35a97b82102a1d1f50a56f8c1bc6533b05a16f6e1a85569c2fe1330ebbd652)
python3 generate_load_data.py --fields 600 --parts 1 --seed 42 --out-dir <dossier>
# Le fichier produit s'appelle agro_load.csv : le renommer en agro_hist.csv.

# Graphe (sommets et arêtes, cohérents avec l'historique : il lit agro_hist.csv)
python3 build_graph_data.py        # options : voir l'en-tête du script
```

### 4.2 Ingestion Logstash vers Elasticsearch

```bash
rm -f ~/tp3_elk/state/sincedb_final           # sinon le fichier est considéré comme déjà lu
cd ~/tp3_elk
python3 scripts/run_ingest.py --run-id FINAL --label "final agro_hist 600 parcelles" \
  --data-glob /home/vm4/tp6_elk/data/history/agro_hist.csv \
  --valid-index agro-final-v1 --reject-index agro-final-rejected-v1 \
  --sincedb /home/vm4/tp3_elk/state/sincedb_final --workers 2
```

- **Résultat attendu :** 51 000 événements sortis, 46 752 valides, 4 248 rejets.
- **Détail des rejets :** DUPLICATE 1 000, puis 812 pour chacune des catégories INVALID_DATE, INVALID_NUMERIC, MISSING_FIELD et UNKNOWN_CATEGORY.
- **Bulk :** 817 réponses HTTP 200.
- **Après un arrêt manuel :** lancer `pkill -f logstash` avant de relancer.

### 4.3 Spark Batch

```bash
cd ~/tp6_elk
spark-submit --driver-memory 1g src/batch/batch_analyses.py
```

- **Résultat :** 46 752 observations valides, 3 248 anomalies détectées, 0 manquée.
- **Analyses produites :** B1 (région × culture), B2 (évolution mensuelle) et B3 (état des capteurs), dans `results/batch/`.

### 4.4 Spark Structured Streaming (deux terminaux)

```bash
# Dossier de démonstration vide
cd ~/tp6_elk && rm -rf stream_demo && mkdir -p stream_demo/{incoming,rejected,output,checkpoint,tmp}

# Terminal A : application de streaming
spark-submit --driver-memory 1g src/streaming/agro_stream.py \
  --base /home/vm4/tp6_elk/stream_demo --input /home/vm4/tp6_elk/stream_demo/incoming

# Terminal B : générateur (60 fichiers, un toutes les 3 s)
python3 scripts/generate_stream.py --out-dir /home/vm4/tp6_elk/stream_demo/incoming \
  --tmp-dir /home/vm4/tp6_elk/stream_demo/tmp --manifest /home/vm4/tp6_elk/stream_demo/manifest_demo.json \
  --total 12000 --batch-size 200 --interval 3 --seed 42 --start-time 2026-10-27T08:00:00

# Vérification, une fois le générateur terminé (Spark toujours lancé)
bash scripts/wait_stream_idle_demo.sh
spark-submit --driver-memory 1g src/streaming/verify_stream_demo.py
```

- **Résultat :** 5 PASS.
  - 12 360 lignes lues, 496 rejets et 11 504 événements valides.
  - 0 doublon en sortie, et la somme des fenêtres vaut 11 504.
- **Reprise :**
  - pour tester, arrêter Spark (Ctrl+C) en plein flux, puis relancer la même commande ;
  - la preuve formelle se trouve dans `06_Resultats/run2_reprise/` (`kill_evidence.txt`, `comparison.txt`).

### 4.5 GraphX

```bash
spark-shell --driver-memory 1g -i src/graphx/agro_graph.scala      # puis :quit
```

- **Graphe :** 755 sommets et 2 879 arêtes ; les 8 contrôles de construction réussissent.
- **Source la plus critique :** WTR-009, avec 34 parcelles dépendantes.
- **Composantes connexes :** 49 sans les sources de secours, 6 avec.
- **Rapport complet :** `results/graphx/graphx_report.txt`.

### 4.6 Intégration et cohérence

```bash
spark-submit --driver-memory 1g src/integration/spark_to_es.py          # 6 index de résultats, 0 erreur
spark-submit --driver-memory 1g src/integration/mission7_coherence.py \
  --history data/history/agro_hist.csv --vertices data/graph/vertices.csv \
  --edges data/graph/edges.csv --stream-events stream/output/events --out results/integration
```

**Résultat :** 15 PASS, 0 FAIL.

### 4.7 Kibana

1. Ouvrir `http://<IP-vm4>:5601`.
2. Aller dans Stack Management → Saved Objects → **Import** et importer `05_Configurations/kibana_dashboard.ndjson`.
3. Ouvrir le tableau de bord « AgroVision – Tableau de bord final ».
4. Régler la période sur `Jan 1, 2026 → now` et le fuseau horaire (Advanced settings `dateFormat:tz`) sur `UTC`.

## 5. Index Elasticsearch

| Index | Documents | Contenu |
|---|---|---|
| agro-final-v1 | 46 752 | observations valides (3 shards, 1 réplique, mapping strict) |
| agro-final-rejected-v1 | 4 248 | rejets avec catégorie, détail et ligne brute |
| agro-spark-batch-b1 | 36 | analyse B1 région × culture |
| agro-spark-batch-b3-capteurs | 3 | analyse B3 par état de capteur |
| agro-stream-events | 11 504 | événements valides du flux |
| agro-stream-metrics | 82 | indicateurs par fenêtre de 5 min et région |
| agro-graphx-sources | 49 | sources d'eau : parcelles, charge pondérée, PageRank |
| agro-graphx-parcelles | 10 | parcelles de plus fort degré |

## 6. Remarques

- **Données :** toutes les données sont synthétiques. Le NDVI est simulé ; aucune image satellite n'est traitée.
- **Sécurité :** aucun mot de passe ni jeton ne figure dans ce dossier, et la sécurité X-Pack est désactivée (laboratoire).
- **Fichiers exclus :** les fichiers d'état (sincedb, file persistante Logstash, checkpoints Spark), les journaux et les caches ont été retirés.
- **Option `--help` :** l'option `--help` de `generate_load_data.py` échoue à cause d'un caractère `%` non échappé dans un texte d'aide. Le script fonctionne normalement avec les options ci-dessus.
