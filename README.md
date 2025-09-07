Dataviz youtube trends

Prérequis
- Python 3.10+
- Pip

Installation
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Ingestion des données
```bash
python ingest_duckdb.py --csv dataset_youtube.csv --table youtube --replace
```

Lancement de l'appli web
```bash
python server.py
# puis ouvrir http://127.0.0.1:8000
```
- Page SQL: `/` (index)
- Carte choroplèthe: `/map.html`
- Carte flux: `/map_links.html`

Variables utiles
- DUCKDB_PATH: chemin du fichier de base (défaut: youtube.duckdb)

Développement
- Code Python: server.py, ingest_duckdb.py
- Web: web/
- Script de nettoyage/CTAS: cleaner.sql

