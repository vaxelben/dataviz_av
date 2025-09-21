#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import datetime
import decimal
from pathlib import Path
import re
from typing import List, Sequence, Tuple

import duckdb
from flask import Flask, jsonify, request, send_from_directory


APP_ROOT = Path(__file__).resolve().parent
WEB_DIR = APP_ROOT / "web"
DEFAULT_DB = os.getenv("DUCKDB_PATH", "youtube.duckdb")

app = Flask(__name__)


def is_select_only(sql: str) -> bool:
    if not sql:
        return False
    # Supprimer espaces multiples et normaliser
    normalized = " ".join(sql.strip().split()).lower()
    # Supporter CTE (WITH) et SELECT
    allowed_starts = ("select ", "with ")
    if not normalized.startswith(allowed_starts):
        return False
    # Interdire mots-clés de modification
    forbidden = (
        "insert ",
        "update ",
        "delete ",
        "create ",
        "drop ",
        "alter ",
        "attach ",
        "copy ",
        "pragma ",
        "call ",
        "grant ",
        "revoke ",
        "vacuum ",
        "checkpoint ",
        "load ",
        "set ",
        "reset ",
        "use ",
        "begin ",
        "commit ",
        "rollback ",
    )
    for token in forbidden:
        if token in normalized:
            return False
    # Autoriser point-virgule final, mais pas plusieurs instructions
    if normalized.count(";") > 1:
        return False
    return True


def is_update_allowed(sql: str) -> bool:
    """Vérifie si une requête UPDATE est autorisée (sécurité basique)."""
    if not sql:
        return False
    
    # Nettoyer et normaliser la requête (gérer les retours à la ligne)
    # Remplacer tous les espaces blancs (espaces, tabs, retours à la ligne) par un seul espace
    normalized = " ".join(sql.strip().split()).lower()
    
    # Autoriser UPDATE, INSERT, DELETE
    allowed_starts = ("update ", "insert ", "delete ")
    if not any(normalized.startswith(start) for start in allowed_starts):
        return False
    
    # Interdire certaines opérations dangereuses
    forbidden_keywords = [
        "drop", "create", "alter", "attach", "pragma", "call", 
        "grant", "revoke", "vacuum", "checkpoint", "load", 
        "reset", "use"
    ]
    
    # Vérifier les mots interdits dans toute la requête
    words = normalized.split()
    for word in words:
        # Nettoyer le mot des caractères de ponctuation
        clean_word = word.strip("();,")
        if clean_word in forbidden_keywords:
            return False
    
    # Autoriser point-virgule final, mais pas plusieurs instructions
    if normalized.count(";") > 1:
        return False
    
    return True


def run_select(db_path: Path, sql: str, max_rows: int = 1000) -> Tuple[List[str], List[Tuple], bool, float]:
    start = time.perf_counter()
    # read_only=True empêche toute modification du fichier DB
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        truncated = False
        if max_rows is not None and len(rows) > max_rows:
            rows = rows[:max_rows]
            truncated = True
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return columns, rows, truncated, elapsed_ms
    finally:
        con.close()


def run_update(db_path: Path, sql: str) -> Tuple[int, float]:
    """Exécute une requête UPDATE/INSERT/DELETE et retourne le nombre de lignes affectées."""
    start = time.perf_counter()
    # read_only=False pour permettre les modifications
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        cur = con.execute(sql)
        # Récupérer le nombre de lignes affectées
        rows_affected = cur.rowcount if cur.rowcount is not None else 0
        con.commit()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return rows_affected, elapsed_ms
    finally:
        con.close()


def validate_identifier(name: str) -> str:
    """Valide un identifiant SQL simple (table/colonne) pour éviter l'injection."""
    if not name:
        raise ValueError("Identifiant vide")
    if not (name[0].isalpha() or name[0] == "_"):
        raise ValueError("Identifiant doit commencer par une lettre ou underscore")
    for ch in name:
        if not (ch.isalnum() or ch == "_"):
            raise ValueError("Identifiant invalide (autorisé: lettres, chiffres, underscore)")
    return name


def validate_yyyy_mm_dd(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        raise ValueError("format attendu YYYY-MM-DD")
    return value


@app.route("/api/query", methods=["POST"])
def api_query():
    data = request.get_json(silent=True) or {}
    sql = data.get("sql", "")
    max_rows = int(data.get("maxRows", 1000))
    db_path = Path(data.get("db") or DEFAULT_DB)

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400
    if not is_select_only(sql):
        return jsonify({"error": "Seules les requêtes SELECT/WITH sont autorisées."}), 400
    try:
        cols, rows, truncated, elapsed_ms = run_select(db_path, sql, max_rows=max_rows)

        def _to_json_safe(value):
            # Convertit les types non JSON en chaînes lisibles
            if value is None:
                return None
            if isinstance(value, (str, int, float, bool)):
                return value
            if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
                # Utiliser ISO 8601
                return value.isoformat()
            if isinstance(value, decimal.Decimal):
                # Préserver la précision en chaîne
                return str(value)
            # Fallback générique
            return str(value)

        safe_rows = [[_to_json_safe(cell) for cell in row] for row in rows]
        return jsonify(
            {
                "columns": cols,
                "rows": safe_rows,
                "rowCount": len(rows),
                "truncated": truncated,
                "elapsedMs": round(elapsed_ms, 2),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/update", methods=["POST"])
def api_update():
    """Endpoint pour exécuter des commandes UPDATE/INSERT/DELETE."""
    data = request.get_json(silent=True) or {}
    sql = data.get("sql", "")
    db_path = Path(data.get("db") or DEFAULT_DB)

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400
    if not is_update_allowed(sql):
        return jsonify({"error": "Seules les requêtes UPDATE/INSERT/DELETE sont autorisées."}), 400
    try:
        rows_affected, elapsed_ms = run_update(db_path, sql)
        return jsonify(
            {
                "rowsAffected": rows_affected,
                "elapsedMs": round(elapsed_ms, 2),
                "message": f"{rows_affected} ligne(s) affectée(s)"
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/agg/country", methods=["GET"])
def api_agg_country():
    """Retourne le nombre de video_id distincts par pays.

    Paramètres optionnels:
      - table: nom de la table source (défaut: yt_clean)
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    # On tolère l'absence de table clean en retombant sur 'youtube'
    # Filtres temporels optionnels
    try:
        on_date = validate_yyyy_mm_dd(request.args.get("onDate"))
        start_date = validate_yyyy_mm_dd(request.args.get("startDate"))
        end_date = validate_yyyy_mm_dd(request.args.get("endDate"))
    except Exception as e:
        return jsonify({"error": f"Paramètres de date invalides: {e}"}), 400

    if on_date:
        where_sql = f"video_trending_date = DATE '{on_date}'"
    else:
        where_parts = ["1=1"]
        if start_date:
            where_parts.append(f"video_trending_date >= DATE '{start_date}'")
        if end_date:
            where_parts.append(f"video_trending_date <= DATE '{end_date}'")
        where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT video_trending_country AS country,
               COUNT(DISTINCT video_id) AS cnt
        FROM {table}
        WHERE {where_sql}
        GROUP BY 1
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=100000)
    except Exception as e:
        # fallback si la table n'existe pas
        try:
            fallback_table = "youtube"
            _ = validate_identifier(fallback_table)
            # Fallback sans filtre de date (la colonne typée peut ne pas exister)
            sql_fb = f"""
                SELECT video_trending_country AS country,
                       COUNT(DISTINCT video_id) AS cnt
                FROM {fallback_table}
                GROUP BY 1
            """
            cols, rows, _, _ = run_select(db_path, sql_fb, max_rows=100000)
        except Exception as e2:
            return jsonify({"error": f"Echec agrégation: {e2}"}), 400

    data = [{"country": r[0], "count": int(r[1]) if r[1] is not None else 0} for r in rows]
    total = sum(d["count"] for d in data)
    max_count = max((d["count"] for d in data), default=0)
    return jsonify({"data": data, "total": total, "max": max_count})


@app.route("/")
def index():
    return send_from_directory(WEB_DIR, "index.html")


@app.route("/map.html")
def map_html():
    return send_from_directory(WEB_DIR, "map.html")


@app.route("/api/flow/country", methods=["GET"])
def api_flow_country():
    """Retourne les flux channel_country -> video_trending_country.

    Réponse: { data: [{src, dst, count}], total: N }
    Paramètres: table (défaut: yt_clean), db (optionnel)
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    try:
        on_date = validate_yyyy_mm_dd(request.args.get("onDate"))
        start_date = validate_yyyy_mm_dd(request.args.get("startDate"))
        end_date = validate_yyyy_mm_dd(request.args.get("endDate"))
    except Exception as e:
        return jsonify({"error": f"Paramètres de date invalides: {e}"}), 400

    base_parts = [
        "channel_country IS NOT NULL",
        "video_trending_country IS NOT NULL",
        "length(trim(channel_country)) > 0",
        "length(trim(video_trending_country)) > 0",
    ]
    if on_date:
        base_parts.append(f"video_trending_date = DATE '{on_date}'")
    else:
        if start_date:
            base_parts.append(f"video_trending_date >= DATE '{start_date}'")
        if end_date:
            base_parts.append(f"video_trending_date <= DATE '{end_date}'")

    # Mode de flux: 'international' (src != dst) ou 'domestic' (src = dst)
    mode = (request.args.get("mode") or "").strip().lower()
    if mode in ("on", "international"):
        base_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        # Par défaut: domestique
        base_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    # Filtre multi-source (liste de channel_country)
    src_list = request.args.getlist("src")
    if not src_list:
        src_csv = request.args.get("srcCsv")
        if src_csv:
            src_list = [s.strip() for s in src_csv.split(",") if s.strip()]
    if src_list:
        def _esc(v: str) -> str:
            return v.replace("'", "''").strip().lower()
        lowered = [_esc(s) for s in src_list if s]
        if lowered:
            in_clause = ",".join([f"'{s}'" for s in lowered])
            base_parts.append(f"lower(trim(channel_country)) IN ({in_clause})")

    where_sql = " AND ".join(base_parts)

    # Dédupliquer par vidéo dans chaque couple (src,dst) à la date, puis sommer les vues
    # Cela évite de compter plusieurs fois les vues si plusieurs lignes existent pour un même video_id
    sql = f"""
        WITH base AS (
          SELECT
            channel_country AS src,
            video_trending_country AS dst,
            video_id,
            MAX(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)) AS views
          FROM {table}
          WHERE {where_sql}
          GROUP BY 1,2,3
        )
        SELECT src, dst,
               COUNT(*) AS cnt,
               COALESCE(SUM(views), 0) AS views
        FROM base
        GROUP BY 1,2
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=200000)
    except Exception as e:
        # fallback si la table 'yt_clean' n'existe pas -> 'youtube'
        try:
            fallback_table = "youtube"
            _ = validate_identifier(fallback_table)
            # Fallback sans filtre de date
            sql_fb = f"""
                SELECT channel_country AS src,
                       video_trending_country AS dst,
                       COUNT(DISTINCT video_id) AS cnt,
                       COALESCE(SUM(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) AS views
                FROM {fallback_table}
                WHERE channel_country IS NOT NULL AND video_trending_country IS NOT NULL
                  AND length(trim(channel_country)) > 0 AND length(trim(video_trending_country)) > 0
                GROUP BY 1,2
            """
            cols, rows, _, _ = run_select(db_path, sql_fb, max_rows=200000)
        except Exception as e2:
            return jsonify({"error": f"Echec flux: {e2}"}), 400

    data = [{
        "src": r[0],
        "dst": r[1],
        "count": int(r[2]) if r[2] is not None else 0,
        "views": int(r[3]) if r[3] is not None else 0,
    } for r in rows]
    total = sum(d["count"] for d in data)
    total_views = sum(d["views"] for d in data)
    return jsonify({"data": data, "total": total, "totalViews": total_views})


@app.route("/map_links.html")
def map_links_html():
    return send_from_directory(WEB_DIR, "map_links.html")


@app.route("/public/<path:filename>")
def public_files(filename: str):
    return send_from_directory(WEB_DIR / "public", filename)


@app.route("/api/meta/date_range", methods=["GET"])
def api_meta_date_range():
    """Retourne min/max de video_trending_date pour la table donnée.

    Réponse: { min: 'YYYY-MM-DD'|null, max: 'YYYY-MM-DD'|null }
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    sql = f"""
        SELECT MIN(video_trending_date)::VARCHAR, MAX(video_trending_date)::VARCHAR
        FROM {table}
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=1)
        if rows and len(rows[0]) == 2:
            return jsonify({"min": rows[0][0], "max": rows[0][1]})
        return jsonify({"min": None, "max": None})
    except Exception:
        # Fallback si erreur (ex: colonne absente)
        return jsonify({"min": None, "max": None})


@app.route("/api/meta/dates", methods=["GET"])
def api_meta_dates():
    """Retourne la liste triée des dates distinctes (YYYY-MM-DD) présentes dans video_trending_date."""
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    sql = f"""
        SELECT DISTINCT video_trending_date::VARCHAR AS d
        FROM {table}
        WHERE video_trending_date IS NOT NULL
        ORDER BY 1
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=100000)
        dates = [r[0] for r in rows if r and r[0]]
        return jsonify({"dates": dates})
    except Exception:
        return jsonify({"dates": []})


@app.route("/favicon.ico")
def favicon():
    # Fallback pour éviter un 404 bruyant dans la console navigateur
    return ("", 204)


@app.route("/api/meta/channels", methods=["GET"])
def api_meta_channels():
    """Retourne la liste triée des channel_country distincts."""
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    sql = f"""
        SELECT DISTINCT channel_country
        FROM {table}
        WHERE channel_country IS NOT NULL AND length(trim(channel_country)) > 0
        ORDER BY 1
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=100000)
        countries = [r[0] for r in rows if r and r[0]]
        return jsonify({"countries": countries})
    except Exception:
        return jsonify({"countries": []})


@app.route("/api/videos/all", methods=["GET"])
def api_videos_all():
    """Retourne toutes les vidéos uniques de la base de données, sans filtre de date.
    
    Paramètres: src=channel_country (optionnel), mode (on/off), table (défaut: yt_clean)
    Réponse: { data: [{ video_id, video_title, views, likes, src, dst }], rowCount }
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    src = (request.args.get("src") or "").strip()
    mode = (request.args.get("mode") or "").strip().lower()

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    where_parts = ["1=1"]  # Pas de filtre de date
    if src:
        esc = src.replace("'", "''").lower()
        where_parts.append(f"lower(channel_country) = '{esc}'")
    if mode in ("on", "international"):
        where_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        where_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
          video_id,
          COALESCE(video_title, '') AS video_title,
          COALESCE(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS views,
          COALESCE(try_cast(regexp_replace(CAST(video_like_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS likes,
          channel_country AS src,
          video_trending_country AS dst
        FROM {table}
        WHERE {where_sql}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY views DESC) = 1
        ORDER BY views DESC
        LIMIT 2000
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=3000)
        data = [
            {
                "video_id": r[0],
                "video_title": r[1],
                "views": int(r[2]) if r[2] is not None else 0,
                "likes": int(r[3]) if r[3] is not None else 0,
                "src": r[4],
                "dst": r[5],
            }
            for r in rows
        ]
        return jsonify({"data": data, "rowCount": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/videos", methods=["GET"])
def api_videos():
    """Retourne les vidéos filtrées par onDate et channel_country, triées par vues décroissantes.

    Paramètres: onDate=YYYY-MM-DD (obligatoire), src=channel_country (optionnel)
    Réponse: { data: [{ video_id, video_title, views, likes, src, dst }], rowCount }
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    try:
        on_date = validate_yyyy_mm_dd(request.args.get("onDate"))
    except Exception as e:
        return jsonify({"error": f"Paramètre onDate invalide: {e}"}), 400
    if not on_date:
        return jsonify({"error": "onDate est requis"}), 400

    src = (request.args.get("src") or "").strip()
    mode = (request.args.get("mode") or "").strip().lower()

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    where_parts = [f"video_trending_date = DATE '{on_date}'"]
    if src:
        esc = src.replace("'", "''").lower()
        where_parts.append(f"lower(channel_country) = '{esc}'")
    if mode in ("on", "international"):
        where_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        where_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
          video_id,
          COALESCE(video_title, '') AS video_title,
          COALESCE(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS views,
          COALESCE(try_cast(regexp_replace(CAST(video_like_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS likes,
          channel_country AS src,
          video_trending_country AS dst
        FROM {table}
        WHERE {where_sql}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY views DESC) = 1
        ORDER BY views DESC
        LIMIT 1000
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=2000)
        data = [
            {
                "video_id": r[0],
                "video_title": r[1],
                "views": int(r[2]) if r[2] is not None else 0,
                "likes": int(r[3]) if r[3] is not None else 0,
                "src": r[4],
                "dst": r[5],
            }
            for r in rows
        ]
        return jsonify({"data": data, "rowCount": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/flow/video", methods=["GET"])
def api_flow_by_video():
    """Retourne les flux pour un video_id donné (option onDate).

    Réponse: { data: [{src, dst, count, views}], total, totalViews }
    Paramètres: videoId (requis), onDate=YYYY-MM-DD (optionnel), table (défaut: yt_clean)
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    video_id_raw = (request.args.get("videoId") or "").strip()
    if not video_id_raw:
        return jsonify({"error": "videoId est requis"}), 400

    try:
        on_date = validate_yyyy_mm_dd(request.args.get("onDate"))
    except Exception as e:
        return jsonify({"error": f"Paramètre onDate invalide: {e}"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    esc_video_id = video_id_raw.replace("'", "''").lower()
    mode = (request.args.get("mode") or "").strip().lower()

    where_parts = [f"lower(video_id) = '{esc_video_id}'"]
    if on_date:
        where_parts.append(f"video_trending_date = DATE '{on_date}'")
    if mode in ("on", "international"):
        where_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        where_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
          channel_country AS src,
          video_trending_country AS dst,
          COUNT(*) AS cnt,
          COALESCE(SUM(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) AS views
        FROM {table}
        WHERE {where_sql}
        GROUP BY 1,2
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=10000)
        data = [
            {
                "src": r[0],
                "dst": r[1],
                "count": int(r[2]) if r[2] is not None else 0,
                "views": int(r[3]) if r[3] is not None else 0,
            }
            for r in rows
        ]
        total = sum(d["count"] for d in data)
        total_views = sum(d["views"] for d in data)
        return jsonify({"data": data, "total": total, "totalViews": total_views})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/videos/by_flow", methods=["GET"])
def api_videos_by_flow():
    """Retourne les vidéos pour un couple src/dst à une date donnée.

    Paramètres: onDate=YYYY-MM-DD (requis), src, dst, table (défaut: yt_clean), mode (on/off)
    Réponse: { data: [{ video_id, video_title, views, likes, src, dst }], rowCount }
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    try:
        on_date = validate_yyyy_mm_dd(request.args.get("onDate"))
    except Exception as e:
        return jsonify({"error": f"Paramètre onDate invalide: {e}"}), 400
    if not on_date:
        return jsonify({"error": "onDate est requis"}), 400

    src = (request.args.get("src") or "").strip()
    dst = (request.args.get("dst") or "").strip()
    mode = (request.args.get("mode") or "").strip().lower()

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    where_parts = [f"video_trending_date = DATE '{on_date}'"]
    if src:
        esc_src = src.replace("'", "''").lower()
        where_parts.append(f"lower(channel_country) = '{esc_src}'")
    if dst:
        esc_dst = dst.replace("'", "''").lower()
        where_parts.append(f"lower(video_trending_country) = '{esc_dst}'")
    if mode in ("on", "international"):
        where_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        where_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
          video_id,
          COALESCE(video_title, '') AS video_title,
          COALESCE(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS views,
          COALESCE(try_cast(regexp_replace(CAST(video_like_count AS VARCHAR), '[^0-9]', '') AS BIGINT), 0) AS likes,
          channel_country AS src,
          video_trending_country AS dst
        FROM {table}
        WHERE {where_sql}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY video_id, channel_country, video_trending_country ORDER BY views DESC) = 1
        ORDER BY views DESC
        LIMIT 2000
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=5000)
        data = [
            {
                "video_id": r[0],
                "video_title": r[1],
                "views": int(r[2]) if r[2] is not None else 0,
                "likes": int(r[3]) if r[3] is not None else 0,
                "src": r[4],
                "dst": r[5],
            }
            for r in rows
        ]
        return jsonify({"data": data, "rowCount": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/video/views_timeline", methods=["GET"])
def api_video_views_timeline():
    """Série temporelle des vues, likes et commentaires par date pour un video_id.

    Paramètres: videoId (requis), table (défaut: yt_clean)
    Réponse: { data: [{ date, views, likes, comments, engagement_rate }], rowCount }
    """
    db_path = Path(request.args.get("db") or DEFAULT_DB)
    table = request.args.get("table", "yt_clean")
    try:
        table = validate_identifier(table)
    except Exception as e:
        return jsonify({"error": f"Nom de table invalide: {e}"}), 400

    video_id_raw = (request.args.get("videoId") or "").strip()
    if not video_id_raw:
        return jsonify({"error": "videoId est requis"}), 400

    if not db_path.exists():
        return jsonify({"error": f"Base DuckDB introuvable: {db_path}"}), 400

    esc_video_id = video_id_raw.replace("'", "''").lower()
    mode = (request.args.get("mode") or "").strip().lower()

    where_parts = [f"lower(video_id) = '{esc_video_id}'", "video_trending_date IS NOT NULL"]
    if mode in ("on", "international"):
        where_parts.append("lower(trim(channel_country)) <> lower(trim(video_trending_country))")
    elif mode in ("off", "domestic", ""):
        where_parts.append("lower(trim(channel_country)) = lower(trim(video_trending_country))")
    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
          video_trending_date::VARCHAR AS d,
          COALESCE(MAX(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) AS views,
          COALESCE(MAX(try_cast(regexp_replace(CAST(video_like_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) AS likes,
          COALESCE(MAX(try_cast(regexp_replace(CAST(video_comment_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) AS comments,
          CASE 
            WHEN COALESCE(MAX(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) > 0 
            THEN ROUND(
              (COALESCE(MAX(try_cast(regexp_replace(CAST(video_like_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0) + 
               COALESCE(MAX(try_cast(regexp_replace(CAST(video_comment_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 0)) * 100.0 
              / COALESCE(MAX(try_cast(regexp_replace(CAST(video_view_count AS VARCHAR), '[^0-9]', '') AS BIGINT)), 1), 4
            )
            ELSE 0 
          END AS engagement_rate
        FROM {table}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """
    try:
        cols, rows, _, _ = run_select(db_path, sql, max_rows=100000)
        data = [
            {
                "date": r[0], 
                "views": int(r[1]) if r[1] is not None else 0,
                "likes": int(r[2]) if r[2] is not None else 0, 
                "comments": int(r[3]) if r[3] is not None else 0,
                "engagement_rate": float(r[4]) if r[4] is not None else 0
            } 
            for r in rows
        ]
        return jsonify({"data": data, "rowCount": len(data)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


def main() -> None:
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    # Conseillé: exécuter depuis la racine du repo pour que WEB_DIR soit correct
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()


