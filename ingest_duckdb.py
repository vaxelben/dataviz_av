#!/usr/bin/env python3
"""
Script d'ingestion DuckDB pour dataset_youtube.csv et exécution de requêtes SQL.

Exemples d'utilisation:
  - Ingestion simple (crée la base youtube.duckdb et la table youtube):
      python ingest_duckdb.py --csv dataset_youtube.csv --table youtube

  - Remplacer la table si elle existe déjà:
      python ingest_duckdb.py --csv dataset_youtube.csv --table youtube --replace

  - Exécuter une requête SQL directement après ingestion:
      python ingest_duckdb.py --query "SELECT COUNT(*) FROM youtube"

  - Exécuter un fichier .sql (plusieurs requêtes séparées par des ;) :
      python ingest_duckdb.py --query-file brief/demo_queries.sql

  - Ouvrir un petit REPL SQL interactif:
      python ingest_duckdb.py --interactive

Notes:
  - Le nom de table est validé (lettres, chiffres, underscore uniquement).
  - L'inférence de schéma est réalisée via read_csv_auto de DuckDB.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Iterable, List, Optional, Sequence, Tuple

import duckdb


def validate_sql_identifier(identifier: str) -> str:
    """Valide un identifiant SQL simple (table) pour éviter l'injection.

    Autorise uniquement lettres, chiffres et underscore, et ne doit pas commencer par un chiffre.
    """
    if not identifier:
        raise ValueError("Le nom de table ne peut pas être vide.")
    if not (identifier[0].isalpha() or identifier[0] == "_"):
        raise ValueError(
            "Le nom de table doit commencer par une lettre ou un underscore."
        )
    for ch in identifier:
        if not (ch.isalnum() or ch == "_"):
            raise ValueError(
                "Le nom de table ne peut contenir que des lettres, chiffres, underscore."
            )
    return identifier


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    query = (
        "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ? LIMIT 1"
    )
    try:
        result = conn.execute(query, [table_name]).fetchone()
        return result is not None
    except Exception:
        return False


def ingest_csv(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    table_name: str,
    replace: bool,
) -> None:
    csv_abs = str(csv_path.resolve())
    validate_sql_identifier(table_name)

    if replace:
        sql = (
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?, SAMPLE_SIZE=-1)"
        )
        conn.execute(sql, [csv_abs])
        return

    # Si on ne remplace pas, on crée la table si elle n'existe pas.
    if table_exists(conn, table_name):
        return

    sql = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?, SAMPLE_SIZE=-1)"
    conn.execute(sql, [csv_abs])


def execute_query(
    conn: duckdb.DuckDBPyConnection, sql: str, *, max_rows: Optional[int] = None
) -> Tuple[List[str], List[Tuple]]:
    cur = conn.execute(sql)
    columns = [desc[0] for desc in (cur.description or [])]
    rows = cur.fetchall()
    if max_rows is not None and len(rows) > max_rows:
        rows = rows[:max_rows]
    return columns, rows


def print_result(columns: Sequence[str], rows: Sequence[Sequence]) -> None:
    if not columns:
        # Instruction de type CREATE/INSERT/etc., rien à afficher
        print("(OK)")
        return

    # Calcul largeur colonnes pour un affichage simple
    col_widths = [len(col) for col in columns]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def fmt_row(values: Sequence) -> str:
        return " | ".join(str(v).ljust(col_widths[i]) for i, v in enumerate(values))

    header = fmt_row(columns)
    sep = "-+-".join("-" * w for w in col_widths)
    print(header)
    print(sep)
    for row in rows:
        print(fmt_row(row))


def execute_sql_file(conn: duckdb.DuckDBPyConnection, sql_file: Path) -> None:
    text = sql_file.read_text(encoding="utf-8")
    # Découpage naïf par ';'. Suffisant pour des requêtes simples.
    statements = [s.strip() for s in text.split(";") if s.strip()]
    for stmt in statements:
        cols, rows = execute_query(conn, stmt)
        print_result(cols, rows)


def interactive_repl(conn: duckdb.DuckDBPyConnection) -> None:
    print("Entrer des requêtes SQL. Tapez .exit ou .quit pour quitter.")
    buffer: List[str] = []
    while True:
        try:
            prompt = "sql> " if not buffer else "...> "
            line = input(prompt)
        except EOFError:
            print()
            break
        line = line.strip()
        if not line:
            continue
        if line in {".exit", ".quit"}:
            break
        buffer.append(line)
        if line.endswith(";"):
            sql = "\n".join(buffer)
            buffer.clear()
            try:
                cols, rows = execute_query(conn, sql)
                print_result(cols, rows)
            except Exception as e:
                print(f"Erreur: {e}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestion CSV -> DuckDB et exécution de requêtes SQL"
    )
    parser.add_argument(
        "--db",
        default="youtube.duckdb",
        help="Chemin de la base DuckDB (fichier). Défaut: youtube.duckdb",
    )
    parser.add_argument(
        "--csv",
        default="dataset_youtube.csv",
        help="Chemin du CSV à ingérer. Défaut: dataset_youtube.csv",
    )
    parser.add_argument(
        "--table",
        default="youtube",
        help="Nom de la table de destination. Défaut: youtube",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Remplacer la table si elle existe déjà",
    )
    parser.add_argument(
        "--query",
        help="Requête SQL à exécuter après ingestion",
    )
    parser.add_argument(
        "--query-file",
        type=str,
        help="Fichier .sql à exécuter (plusieurs instructions séparées par ';')",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Ouvrir un prompt SQL interactif",
    )
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        help="Ne pas (ré)ingérer le CSV, seulement exécuter des requêtes",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    db_path = Path(args.db)
    csv_path = Path(args.csv)
    table_name = args.table

    # Connexion (crée le fichier si absent)
    conn = duckdb.connect(str(db_path))

    try:
        if not args.no_ingest:
            if not csv_path.exists():
                print(f"CSV introuvable: {csv_path}", file=sys.stderr)
                return 1
            print(
                f"Ingestion de {csv_path} dans {db_path} (table {table_name})"
                + (" avec remplacement" if args.replace else "")
            )
            ingest_csv(conn, csv_path, table_name, args.replace)

        # Exécutions SQL éventuelles
        if args.query_file:
            sql_file = Path(args.query_file)
            if not sql_file.exists():
                print(f"Fichier SQL introuvable: {sql_file}", file=sys.stderr)
                return 1
            execute_sql_file(conn, sql_file)

        if args.query:
            cols, rows = execute_query(conn, args.query)
            print_result(cols, rows)

        if args.interactive and not args.query and not args.query_file:
            interactive_repl(conn)

        if not (args.query or args.query_file or args.interactive):
            print(
                "Aucune requête fournie. Utilisez --query, --query-file ou --interactive."
            )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())


