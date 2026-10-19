#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gadgetbridge_to_influxdb - ERWEITERTE Version
=============================================

Lädt einen Gadgetbridge-Datenbank-Export (WebDAV/Nextcloud ODER lokale Datei)
und importiert ALLEN verfügbaren Sensordaten in InfluxDB 2.x.

Basis: https://github.com/bentasker/gadgetbridge_to_influxdb
Copyright (c) 2023 B Tasker, BSD 3-Clause. Die Erweiterungen unten sind neu.

Wesentliche Änderungen gegenüber dem Original:
  * Liest jetzt die Sample-Tabellen ALDER Hersteller (HUAWEI, XIAOMI, MOYOUNG,
    WEAR, GARMIN, LAXAS_FIT, ...). Bekannte Tabellen werden gezielt gelesen,
    alle übrigen *_SAMPLE-Tabellen laufen über einen generischen Catch-All.
  * Fehlende/leere Tabellen sind nicht mehr fatal (Original crashte mit
    sqlite3.OperationalError, wenn eine Tabelle nicht existierte).
  * Per-Tabellen-Report: zeigt exakt, welche Tabellen Daten im
    Abfragefenster haben (oder warum nicht) - wichtig zur Fehlersuche.
  * InfluxDB-Write in Batches statt einem HTTP-Request pro Punkt.
  * Bugfixes:
      - sync_check nutzte die veraltete Schleifenvariable r[1]
      - Punkte ohne gültige Felder werden übersprungen (InfluxDB-Fehlerquelle)
      - Stress-/Schlaf-Minuten-Expansion jetzt begrenzt (Schutz vor
        endlosen Schleifen bei Zeitstempel-Fehlern)
      - defekter Gerätebezug (KeyError bei gelöschten Geräten)
  * Neue Env-Vars: LOCAL_DB, DRY_RUN, GENERIC_TABLES, WRITE_BATCH_SIZE.
  * Zeitstempel-Einheit (s/ms/µs/ns) wird pro Tabelle automatisch erkannt.
"""

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import logging

LOG = logging.getLogger("gb2influx")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- optionale Abhängigkeiten (für LOCAL_DB + DRY_RUN nicht nötig) ---
try:
    from webdav3.client import Client as WebDavClient
except ImportError:
    WebDavClient = None

try:
    from influxdb_client import InfluxDBClient, Point
except ImportError:
    InfluxDBClient = None
    Point = None


### Config section (gleiche Env-Var-Namen wie im Original) ###

# WebDAV-Server (z. B. Nextcloud: https://[nextcloud domain]/remote.php/dav/)
WEBDAV_URL = os.getenv("WEBDAV_URL", "")
WEBDAV_PATH = os.getenv("WEBDAV_PATH", "files/service_user/GadgetBridge/")
WEBDAV_USER = os.getenv("WEBDAV_USER", "")
WEBDAV_PASS = os.getenv("WEBDAV_PASS", "")

# Dateiname des Exports auf dem WebDAV-Server
EXPORT_FILE = os.getenv("EXPORT_FILENAME", "gadgetbridge")

# NEU: Pfad zu einer lokalen SQLite-Datei (überspringt dann WebDAV komplett)
LOCAL_DB = os.getenv("LOCAL_DB", "")

# NEU: Y = Daten extrahieren und reporten, aber NICHT nach InfluxDB schreiben
DRY_RUN = os.getenv("DRY_RUN", "N").upper() == "Y"

# Wie weit in die Vergangenheit wird abgefragt? (Sekunden, Default 24h)
QUERY_DURATION = int(os.getenv("QUERY_DURATION", 86400))

# InfluxDB-Settings
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT", "gadgetbridge")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "testing_db")

# Welche Stunden gelten als Schlafzeit (für Stress-Mittelung)?
SLEEP_HOURS = os.getenv("SLEEP_HOURS", "0,1,2,3,4,5,6").split(",")

# Zum Testen/Debugging: N = lokale Kopie der sqlite-DB behalten
REMOVE_TEMP_DB = os.getenv("REMOVE_TEMP_DB", "Y")
EXPERIMENTAL_OPTS = os.getenv("EXPERIMENTAL_OPTS", "").split(",")

# NEU: Y = unbekannte *_SAMPLE-Tabellen generisch mit importieren (Default)
GENERIC_TABLES = os.getenv("GENERIC_TABLES", "Y").upper() == "Y"

# RAW_KIND (Activity-Tabellen) als Field (Default: eine saubere Serie pro Gerät)
# oder als Tag (wie Original: filterbar im Data Explorer, aber eigene Serie
# pro Wert -> zerhackte Linien / "Datensalat").
RAW_KIND_MODE = os.getenv("RAW_KIND_MODE", "field").lower()

# Generische Tabellen: TEXT-Spalten werden nur dann Tag, wenn sie <= so viele
# verschiedene Werte haben (Tags sind Serien-Spliter!). Der Rest wird
# String-Field (InfluxDB 2.x). 0 = nie TEXT als Tag.
TEXT_AS_TAG_LIMIT = int(os.getenv("TEXT_AS_TAG_LIMIT", "8"))

# RAW_KIND-Werte, die auf Schlaf-Stufen zeigen (Mi-Band) - werden in die
# eigene sample_type=sleep-Serie geroutet
SLEEP_RAW_KINDS = {112, 120, 121, 122, 249}

# NEU: Punkte pro InfluxDB-Write-Request
WRITE_BATCH_SIZE = int(os.getenv("WRITE_BATCH_SIZE", "1000"))

# NEU: Pfad, an den alle übersprungenen Zeilen als JSONL gedumpt werden
# (leer = aus). Im Container z. B. /app/skipped.jsonl + Volume-Mount.
SKIP_DUMP_FILE = os.getenv("SKIP_DUMP_FILE", "")

# Obergrenze für die Per-Minuten-Expansion (Stress/Schlaf), in Minuten
MAX_EXPAND_MINUTES = 24 * 60

# Kandidaten für die Zeitstempel-Spalte einer Tabelle
TS_CANDIDATES = [
    "TIMESTAMP", "START_TIMESTAMP", "FIRST_TIMESTAMP", "LAST_TIMESTAMP",
    "DOWNLOAD_TIMESTAMP", "FILE_TIMESTAMP", "WAKEUP_TIME",
]


### Zeitstempel-Helfer ###

def unit_of(ts):
    """Heuristik: (Divisor-zu-Sekunden, Label) für einen rohen Zeitstempel."""
    if ts is None:
        return None
    if ts >= 1e17:
        return (1e9, "ns")
    if ts >= 1e14:
        return (1e6, "µs")
    if ts >= 1e11:
        return (1e3, "ms")
    if ts >= 1e8:
        return (1.0, "s")
    return None


def to_ns(ts):
    """Rohen Zeitstempel (s/ms/µs/ns) nach Nanosekunden konvertieren."""
    u = unit_of(ts)
    if not u:
        return None
    div, _ = u
    return int(ts / div * 1e9)


# Zeitstempel vor 2000 gelten als unplausibel
_MIN_PLAUSIBLE_NS = 946684800 * 10**9


def to_ns_smart(ts, now):
    """Reparierende Konversion: probiert s/ms/µs/ns durch und liefert die
    Konversion, die in den Plausibilitätsbereich (2000 … jetzt+1 Tag) fällt.
    Das rettet Zeilen aus gemischten Tabellen (z. B. paar Zeilen in s in einer
    ms-Tabelle), die vorher auf unsinnige Daten (Jahr 58.000) geschrieben oder
    verworfen wurden. Liefert (ns, Einheit) oder (None, None)."""
    if ts is None:
        return None, None
    min_ns = _MIN_PLAUSIBLE_NS
    max_ns = (now + 86400) * 10**9
    candidates = []
    primary = unit_of(ts)
    if primary:
        candidates.append(primary)
    for cand in ((1.0, "s"), (1e3, "ms"), (1e6, "µs"), (1e9, "ns")):
        if cand not in candidates:
            candidates.append(cand)
    for div, label in candidates:
        ns = int(ts / div * 1e9)
        if min_ns <= ns <= max_ns:
            return ns, label
    return None, None


def human(ns):
    if not ns:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ns / 1e9))


### DB-Helfer ###

def table_exists(cur, table):
    return cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def get_tables(cur):
    return [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]


def table_columns(cur, table):
    """dict Spaltenname -> SQLite-Typ (klein/schreibweise-normalisiert)."""
    return {r[1]: (r[2] or "").upper()
            for r in cur.execute(f'PRAGMA table_info("{table}")').fetchall()}


def get_devices(cur):
    """Liest die DEVICE-Tabelle robust (nur Spalten, die vorhanden sind)."""
    devices = {}
    if not table_exists(cur, "DEVICE"):
        LOG.error("Keine DEVICE-Tabelle in der Datenbank - falscher Export?")
        return devices
    cols = table_columns(cur, "DEVICE")
    select = ["_id"]
    for wanted in ("NAME", "IDENTIFIER", "ALIAS"):
        if wanted in cols:
            select.append(wanted)
    for r in cur.execute(f"SELECT {', '.join(select)} FROM DEVICE").fetchall():
        vals = dict(zip(select, r))
        dev_id = vals["_id"]
        devices[f"dev-{dev_id}"] = {
            "name": vals.get("NAME") or f"device-{dev_id}",
            "identifier": vals.get("IDENTIFIER") or f"dev-{dev_id}",
            "alias": "Unset" if vals.get("ALIAS") in (None, "") else vals["ALIAS"],
        }
    return devices


def device_tags(devices, dev_id):
    """Device-Tags, ohne KeyError, wenn das Gerät aus der DEVICE-Tabelle
    verschwunden ist (Bugfix gegenüber Original)."""
    key = f"dev-{dev_id}"
    d = devices.get(key)
    if not d:
        return {"device": key, "identifier": key, "alias": "Unknown"}
    return {"device": d["name"], "identifier": d["identifier"], "alias": d["alias"]}


def window_boundary(cur, table, ts_col, now):
    """Liefert (Divisor, Einheit-Label, Boundary-in-Einheit, max_ts-roh)."""
    row = cur.execute(f'SELECT MAX("{ts_col}") FROM "{table}"').fetchone()
    max_ts = row[0] if row else None
    u = unit_of(max_ts) or (1.0, "s")
    div, label = u
    # 120s Puffer: gleicht Rundungs-/Uhren-Differenzen zwischen
    # Telefon (Stempelquelle) und Container (Abfragezeit) aus
    boundary = int((now - QUERY_DURATION - 120) * div)
    return div, label, boundary, max_ts


def build_window_queries(table, ts_col, boundary, now, select_cols, label="s"):
    """Primär-Query (Fenster) + Reparatur-Query für Misch-Einheiten.

    Reparatur-Query: Zeilen, deren RAW-Wert unterhalb der Fenster-Grenze
    liegt (also z. B. paar Zeilen in Sekunden in einer ms-Tabelle) und die
    damit vom Primärfilter verpasst würden. Nur relevant, wenn die Einheit
    der Tabelle ms/µs/ns ist (bei s ist die Grenze sowieso klein genug).
    Die beiden Mengen sind disjunkt (>= vs. < boundary).
    """
    sel = f"{', '.join(select_cols)}" if isinstance(select_cols, (list, tuple)) else select_cols
    main_q = (f'SELECT {sel} FROM "{table}" WHERE "{ts_col}" >= ? '
              f'ORDER BY "{ts_col}"')
    repair_q = None
    repair_params = None
    if label in ("ms", "µs", "ns"):
        # Werte, die als SEKUNDEN-Werte in das Fenster fallen
        repair_q = (f'SELECT {sel} FROM "{table}" '
                    f'WHERE "{ts_col}" < ? AND "{ts_col}" >= ? '
                    f'ORDER BY "{ts_col}"')
        repair_params = (boundary, int(now - QUERY_DURATION - 120))
    return main_q, (boundary,), repair_q, repair_params


def make_row(ts_ns, fields, tags):
    return {"timestamp": ts_ns, "fields": dict(fields), "tags": dict(tags)}


class Stats:
    """Pro-Tabellen-Statistik für den Report."""

    def __init__(self):
        self.tables = {}

    def note(self, table, rows=None, unit=None, note="", max_ts_ns=None):
        t = self.tables.setdefault(
            table, {"rows": 0, "unit": "", "note": "", "max": ""})
        if rows is not None:
            t["rows"] += rows
        if unit:
            t["unit"] = unit
        if note:
            t["note"] = (t["note"] + "; " + note).strip("; ") if t["note"] else note
        if max_ts_ns:
            t["max"] = human(max_ts_ns)

    def report(self):
        LOG.info("=== Tabellen-Report (Zeilen im Abfragefenster) ===")
        for table in sorted(self.tables):
            t = self.tables[table]
            extra = f"  unit={t['unit']}" if t["unit"] else ""
            mx = f"  max={t['max']}" if t["max"] else ""
            note = f"  [{t['note']}]" if t["note"] else ""
            LOG.info("  %-45s %7d rows%s%s%s", table, t["rows"], extra, mx, note)


def observe(ctx, dev_id, ts_ns):
    if ts_ns is None or dev_id is None:
        return
    key = f"dev-{dev_id}"
    ctx["devices_observed"][key] = max(
        ctx["devices_observed"].get(key, 0), ts_ns)


class Skips:
    """Verfolgt alle übersprungenen Zeilen (Tabelle, Grund, Beispiel)."""

    def __init__(self):
        self.counts = {}   # (table, reason) -> Anzahl
        self.examples = {} # (table, reason) -> [rows]

    def note(self, table, reason, row=None, max_examples=3):
        key = (table, reason)
        self.counts[key] = self.counts.get(key, 0) + 1
        ex = self.examples.setdefault(key, [])
        if row is not None and len(ex) < max_examples:
            ex.append(row)

    def report(self):
        if not self.counts:
            LOG.info("Keine Zeilen übersprungen.")
            return
        total = sum(self.counts.values())
        LOG.warning("=== Übersprungene Zeilen: %d gesamt ===", total)
        for (table, reason) in sorted(self.counts):
            LOG.warning("  %-45s %6d  [%s]", table, self.counts[(table, reason)],
                        reason)
            for ex in self.examples.get((table, reason), []):
                LOG.warning("      Beispiel: %s", ex)
        if SKIP_DUMP_FILE:
            try:
                with open(SKIP_DUMP_FILE, "w", encoding="utf-8") as f:
                    for (table, reason), rows in self.examples.items():
                        for row in rows:
                            f.write(json.dumps(
                                {"table": table, "reason": reason, "row": row},
                                default=str) + "\n")
                LOG.warning("Skip-Beispiele nach %s geschrieben", SKIP_DUMP_FILE)
            except OSError as e:
                LOG.warning("Skip-Dump nicht möglich: %s", e)


def ctx_make(cur, devices, now, stats, skips):
    return {
        "cur": cur, "devices": devices, "now": now, "stats": stats,
        "skips": skips,
        "results": [], "devices_observed": {},
    }


### Spezifikations-Engine für bekannte Tabellen ###
# fields: Feldname -> Kandidaten-Spalten (erste vorhandene wird genommen)
# tags:   Tag-Name  -> Kandidaten-Spalten

ALL_SPECS = {
    # ---- Xiaomi/Amazfit (Huami) - wie im Original ----
    "HUAMI_SPO2_SAMPLE": {
        "fields": {"spo2": ["SPO2"]},
        "tags": {"type_num": ["TYPE_NUM"]},
        "sample_type": "huami_spo2",
    },
    "HUAMI_SLEEP_RESPIRATORY_RATE_SAMPLE": {
        "fields": {"sleep_respiratory_rate": ["RATE"]},
        "tags": {},
        "sample_type": "huami_sleep_respiratory_rate",
    },
    "HUAMI_PAI_SAMPLE": {
        "fields": {
            "pai_low": ["PAI_LOW"], "pai_moderate": ["PAI_MODERATE"],
            "pai_high": ["PAI_HIGH"], "time_low": ["TIME_LOW"],
            "time_moderate": ["TIME_MODERATE"], "time_high": ["TIME_HIGH"],
            "pai_today": ["PAI_TODAY"], "pai_total": ["PAI_TOTAL"],
        },
        "tags": {},
        "sample_type": "huami_pai",
    },
    "HUAMI_HEART_RATE_MANUAL_SAMPLE": {
        "fields": {"heart_rate": ["HEART_RATE"]},
        "tags": {}, "sample_type": "heart_rate_manual",
    },
    "HUAMI_HEART_RATE_MAX_SAMPLE": {
        "fields": {"heart_rate": ["HEART_RATE"]},
        "tags": {}, "sample_type": "heart_rate_max",
    },
    "HUAMI_HEART_RATE_RESTING_SAMPLE": {
        "fields": {"heart_rate": ["HEART_RATE"]},
        "tags": {}, "sample_type": "heart_rate_resting",
    },
    # MI_BAND + HUAMI_EXTENDED werden in extract_activity_tables() behandelt
    # (RAW_KIND-Handling + Schlaf-Routing).
    # ---- herstellerunabhängig ----
    "BATTERY_LEVEL": {
        "fields": {"battery_level": ["LEVEL"]},
        "tags": {"battery": ["BATTERY_INDEX"]},
        "sample_type": "battery",
    },
    # ---- Huawei ----
    "HUAWEI_ACTIVITY_SAMPLE": {
        # raw_kind als FIELD (sonst Serie-Splitting wie beim Mi-Band)
        "fields": {
            "steps": ["STEPS"], "calories": ["CALORIES"],
            "distance": ["DISTANCE"], "spo2": ["SPO2", "SPO"],
            "heart_rate": ["HEART_RATE"],
            "resting_heart_rate": ["RESTING_HEART_RATE"],
            "raw_kind": ["RAW_KIND"],
        },
        "tags": {"source": ["SOURCE"]},
        "sample_type": "huawei_activity",
    },
    "HUAWEI_HRV_VALUE_SAMPLE": {
        "fields": {"hrv": ["VALUE"]},
        "tags": {}, "sample_type": "huawei_hrv",
    },
    "HUAWEI_EMOTIONS_SAMPLE": {
        "fields": {"valence": ["VALENCE_CHARACTER"],
                   "arousal": ["AROUSAL_CHARACTER"]},
        "tags": {"emotion_status": ["STATUS"]},
        "sample_type": "huawei_emotions",
    },
    "HUAWEI_TEMPERATURE_SAMPLE": {
        "fields": {"body_temperature": ["BODY_TEMPERATURE", "TEMPERATURE"]},
        "tags": {}, "sample_type": "huawei_temperature",
    },
    "HUAWEI_WORKOUT_SUMMARY_SAMPLE": {
        "ts": "START_TIMESTAMP",
        "fields": {
            "calories": ["CALORIES"], "distance": ["DISTANCE"],
            "step_count": ["STEP_COUNT"], "total_time": ["TOTAL_TIME"],
            "duration": ["DURATION"], "strokes": ["STROKES"],
            "avg_stroke_rate": ["AVG_STROKE_RATE"],
            "pool_length": ["POOL_LENGTH"], "laps": ["LAPS"],
            "avg_swolf": ["AVG_SWOLF"], "max_altitude": ["MAX_ALTITUDE"],
            "min_altitude": ["MIN_ALTITUDE"],
            "elevation_gain": ["ELEVATION_GAIN"],
            "elevation_loss": ["ELEVATION_LOSS"],
            "workout_load": ["WORKOUT_LOAD"],
            "workout_aerobic_effect": ["WORKOUT_AEROBIC_EFFECT"],
            "workout_anaerobic_effect": ["WORKOUT_ANAEROBIC_EFFECT"],
            "recovery_time": ["RECOVERY_TIME"],
            # workout_number als FIELD (sonst 1 Serie pro Workout)
            "workout_number": ["WORKOUT_NUMBER"],
            "min_heart_rate_peak": ["MIN_HEART_RATE_PEAK"],
            "max_heart_rate_peak": ["MAX_HEART_RATE_PEAK"],
            "avg_heart_rate": ["AVG_HEART_RATE"],
            "max_heart_rate": ["MAX_HEART_RATE"],
            "min_heart_rate": ["MIN_HEART_RATE"],
        },
        "tags": {
            "workout_type": ["TYPE"], "status": ["STATUS"],
        },
        "sample_type": "huawei_workout",
    },
}


def run_spec(ctx, table, spec):
    """Verarbeitet eine Tabelle nach Spezifikation.
    Liefert True, wenn die Tabelle damit abgedeckt ist."""
    cur = ctx["cur"]
    if not table_exists(cur, table):
        ctx["stats"].note(table, note="Tabelle nicht vorhanden")
        return True

    cols = table_columns(cur, table)
    ts_col = spec.get("ts") or next((c for c in TS_CANDIDATES if c in cols), None)
    if not ts_col:
        ctx["stats"].note(table, note="keine Zeitstempel-Spalte - generisch?")
        return False

    div, label, boundary, max_ts = window_boundary(cur, table, ts_col, ctx["now"])
    max_ns = to_ns(max_ts)

    resolved_f = {}
    for fname, candidates in spec["fields"].items():
        for c in candidates:
            if c in cols:
                resolved_f[c] = fname
                break
    if not resolved_f:
        ctx["stats"].note(table, note="keine bekannten Felder - wird generisch")
        return False

    resolved_t = {}
    for tname, candidates in (spec.get("tags") or {}).items():
        for c in candidates:
            if c in cols:
                resolved_t[c] = tname
                break

    select_cols = [ts_col] + list(resolved_f) + list(resolved_t)
    if "DEVICE_ID" in cols and "DEVICE_ID" not in select_cols:
        select_cols.append("DEVICE_ID")

    main_q, main_p, repair_q, repair_p = build_window_queries(
        table, ts_col, boundary, ctx["now"], select_cols, label)
    n = skipped = repaired = 0
    queries = [(main_q, main_p)]
    if repair_q:
        queries.append((repair_q, repair_p))
    for q, qp in queries:
        for r in cur.execute(q, qp).fetchall():
            d = dict(zip(select_cols, r))
            ts_ns, used_unit = to_ns_smart(d[ts_col], ctx["now"])
            if ts_ns is None:
                ctx["skips"].note(table, "unplausibler Zeitstempel",
                                  {ts_col: d[ts_col]})
                skipped += 1
                continue
            if used_unit and used_unit != label:
                repaired += 1
            fields = {}
            for c, fname in resolved_f.items():
                v = d.get(c)
                if v is None:
                    continue
                if v == -1:  # Gadgetbridge verwendet -1 als "kein Wert"
                    continue
                fields[fname] = v
            if not fields:
                ctx["skips"].note(table, "alle Felder -1/leer", d)
                continue
            tags = {}
            if "DEVICE_ID" in d and d["DEVICE_ID"] is not None:
                tags = device_tags(ctx["devices"], d["DEVICE_ID"])
                observe(ctx, d["DEVICE_ID"], ts_ns)
            for c, tname in resolved_t.items():
                v = d.get(c)
                if v is not None:
                    tags[tname] = str(v)
            tags["sample_type"] = spec.get("sample_type", table.lower())
            ctx["results"].append(make_row(ts_ns, fields, tags))
            n += 1

    note = f"{skipped} Zeilen verworfen (ungültiger Zeitstempel)" if skipped else ""
    if repaired:
        note = (note + "; " if note else "") + \
            f"{repaired} Zeilen mit reparierter Zeitstempel-Einheit"
    if n == 0 and max_ts is not None:
        note = (note + "; " if note else "") + \
            f"Tabelle HAT Daten, aber KEINE im Abfragefenster! " \
            f"Neueste Zeile: {human(to_ns(max_ts))} UTC " \
            f"(Fenster beginnt {human(int((ctx['now'] - QUERY_DURATION - 120) * 1e9))} UTC)"
    ctx["stats"].note(table, rows=n, unit=label, note=note, max_ts_ns=max_ns)
    return True


### Sonderlogik: Activity-Tabellen (RAW_KIND + Schlaf-Routing) ###

def extract_activity_tables(ctx):
    """Behandelt MI_BAND_ACTIVITY_SAMPLE und HUAMI_EXTENDED_ACTIVITY_SAMPLE.

    RAW_KIND als Tag splittet jede Serie pro Wert (InfluxDB-Serien-Splitting)
    -> "Datensalat". Default: raw_kind wird ein Field (eine saubere Serie
    pro Gerät). Schlaf-RAW_KINDs (112,120-122,249) werden in die eigene
    sample_type=sleep-Serie geroutet, damit sie die HR/Steps-Linien nicht
    verunreinigen.
    """
    for table, extra_fields in (
        ("MI_BAND_ACTIVITY_SAMPLE", ()),
        ("HUAMI_EXTENDED_ACTIVITY_SAMPLE",
         ("SLEEP", "DEEP_SLEEP", "REM_SLEEP")),
    ):
        cur = ctx["cur"]
        if not table_exists(cur, table):
            ctx["stats"].note(table, note="Tabelle nicht vorhanden")
            continue
        cols = table_columns(cur, table)
        div, label, boundary, max_ts = window_boundary(cur, table, "TIMESTAMP", ctx["now"])
        select_cols = ["TIMESTAMP", "RAW_INTENSITY", "STEPS", "RAW_KIND", "HEART_RATE"]
        for c in extra_fields:
            if c in cols:
                select_cols.append(c)
        if "DEVICE_ID" in cols:
            select_cols.append("DEVICE_ID")
        main_q, main_p, repair_q, repair_p = build_window_queries(
            table, "TIMESTAMP", boundary, ctx["now"], select_cols, label)
        n = n_sleep = 0
        queries = [(main_q, main_p)]
        if repair_q:
            queries.append((repair_q, repair_p))
        for q, qp in queries:
            for r in cur.execute(q, qp).fetchall():
                d = dict(zip(select_cols, r))
                ts_ns, _u = to_ns_smart(d["TIMESTAMP"], ctx["now"])
                if ts_ns is None:
                    ctx["skips"].note(table, "unplausibler Zeitstempel",
                                      {"TIMESTAMP": d["TIMESTAMP"]})
                    continue
                fields = {}
                for c, fname in (("RAW_INTENSITY", "intensity"), ("STEPS", "steps"),
                                 ("HEART_RATE", "heart_rate"),
                                 ("SLEEP", "sleep"), ("DEEP_SLEEP", "deep_sleep"),
                                 ("REM_SLEEP", "rem_sleep")):
                    if c in d and d[c] is not None and d[c] != -1:
                        fields[fname] = d[c]
                raw_kind = d["RAW_KIND"]
                if raw_kind is not None:
                    if RAW_KIND_MODE == "tag":
                        pass  # wird unten als Tag geschrieben
                    else:
                        fields["raw_kind"] = raw_kind
                is_sleep = raw_kind in SLEEP_RAW_KINDS
                if is_sleep and not fields:
                    fields["raw_kind"] = raw_kind
                if not fields:
                    ctx["skips"].note(table, "alle Felder -1/leer", d)
                    continue
                tags = device_tags(ctx["devices"], d["DEVICE_ID"])
                observe(ctx, d["DEVICE_ID"], ts_ns)
                if is_sleep:
                    tags["sample_type"] = "sleep"
                    tags["sleep"] = "raw_kind"
                    n_sleep += 1
                else:
                    tags["sample_type"] = ("activity" if table == "HUAMI_EXTENDED_ACTIVITY_SAMPLE"
                                           else "periodic_samples")
                    if RAW_KIND_MODE == "tag":
                        tags["raw_kind"] = str(raw_kind)
                ctx["results"].append(make_row(ts_ns, fields, tags))
                n += 1
        note = f"davon {n_sleep} Schlaf-RAW_KINDs -> sleep-Serie" if n_sleep else ""
        if RAW_KIND_MODE == "field":
            note = (note + "; " if note else "") + "raw_kind als Field (keine Serie-Splitting)"
        ctx["stats"].note(table, rows=n, unit=label, note=note,
                          max_ts_ns=to_ns(max_ts))


### Sonderlogik: Stress mit Per-Minuten-Expansion (wie Original, begrenzt) ###

def stress_level_name(stress):
    # Schwellwerte aus der Zepp-App (wie im Original)
    if stress <= 39:
        return "stress_level_counter_relaxed"
    if 40 <= stress <= 59:
        return "stress_level_counter_normal"
    if 60 <= stress <= 79:
        return "stress_level_counter_medium"
    if stress >= 80:
        return "stress_level_counter_high"
    return "stress_level_counter_unknown"


def extract_huami_stress(ctx):
    table = "HUAMI_STRESS_SAMPLE"
    cur = ctx["cur"]
    if not table_exists(cur, table):
        ctx["stats"].note(table, note="Tabelle nicht vorhanden")
        return
    div, label, boundary, max_ts = window_boundary(cur, table, "TIMESTAMP", ctx["now"])
    sel = ("TIMESTAMP, DEVICE_ID, TYPE_NUM, STRESS, "
           "LEAD (TIMESTAMP, 1) OVER (PARTITION BY DEVICE_ID, USER_ID, TYPE_NUM "
           f"ORDER BY TIMESTAMP) NEXT_TS")
    main_q = f'SELECT {sel} FROM "{table}" WHERE TIMESTAMP >= ? ORDER BY TIMESTAMP'
    main_p = (boundary,)
    repair_q = None
    repair_p = None
    if label in ("ms", "µs", "ns"):
        repair_q = (f'SELECT {sel} FROM "{table}" '
                    f'WHERE TIMESTAMP < ? AND TIMESTAMP >= ? ORDER BY TIMESTAMP')
        repair_p = (boundary, int(ctx["now"] - QUERY_DURATION - 120))
    n = 0
    queries = [(main_q, main_p)]
    if repair_q:
        queries.append((repair_q, repair_p))
    for q, qp in queries:
        for r in cur.execute(q, qp).fetchall():
            ts_ns, _u = to_ns_smart(r[0], ctx["now"])
            if ts_ns is None:
                ctx["skips"].note(table, "unplausibler Zeitstempel",
                                  {"TIMESTAMP": r[0]})
                continue
            stress = r[3]
            if stress is None or stress == -1:
                ctx["skips"].note(table, "alle Felder -1/leer", {"STRESS": stress})
                continue
            tags = device_tags(ctx["devices"], r[1])
            tags["type_num"] = str(r[2])
            fields = {"stress": stress}
            if str(time.gmtime(ts_ns / 1e9).tm_hour) not in SLEEP_HOURS:
                fields["stress_exc_sleep"] = stress
            ctx["results"].append(make_row(ts_ns, fields, tags))
            observe(ctx, r[1], ts_ns)
            n += 1

            # Per-Minuten-Expansion über das Zeitfenster bis zum nächsten Sample
            next_ts = r[4]
            if next_ts is None:
                continue
            end_ns, _ = to_ns_smart(next_ts, ctx["now"])
            if end_ns is None or end_ns <= ts_ns:
                continue
            if (end_ns - ts_ns) / 1e9 / 60 > MAX_EXPAND_MINUTES:
                ctx["stats"].note(table, note="Expansion begrenzt (längere Lücke)")
                continue
            sname = stress_level_name(stress)
            t_s = ts_ns / 1e9
            end_s = end_ns / 1e9
            while t_s < end_s:
                f2 = {"current_stress_level": stress, sname: 1}
                if str(time.gmtime(t_s).tm_hour) not in SLEEP_HOURS:
                    f2[f"{sname}_exc_sleep"] = 1
                t2 = dict(tags)
                t2["stress"] = "point_in_time"
                ctx["results"].append(make_row(int(t_s * 1e9), f2, t2))
                t_s += 60
    ctx["stats"].note(table, rows=n, unit=label, max_ts_ns=to_ns(max_ts))


### Sonderlogik: Schlafdaten (experimentell, wie Original + Zeitfenster) ###

def get_sleep_data(ctx):
    table = "MI_BAND_ACTIVITY_SAMPLE"
    cur = ctx["cur"]
    if not table_exists(cur, table):
        ctx["stats"].note(table, note="sleep: Tabelle nicht vorhanden")
        return
    div, label, boundary, max_ts = window_boundary(cur, table, "TIMESTAMP", ctx["now"])
    q = (f"SELECT TIMESTAMP, DEVICE_ID, RAW_INTENSITY, RAW_KIND, "
         "LEAD (TIMESTAMP, 1) OVER (PARTITION BY DEVICE_ID, USER_ID "
         f"ORDER BY TIMESTAMP) NEXT_TS FROM \"{table}\" "
         f"WHERE TIMESTAMP >= ? AND "
         "(RAW_KIND=112 OR RAW_KIND BETWEEN 120 AND 122 OR RAW_KIND=249) "
         "ORDER BY TIMESTAMP")
    mapping = {120: "light", 121: "deep", 122: "REM", 249: "very_light", 112: "waking"}
    n = 0
    for r in cur.execute(q, (boundary,)).fetchall():
        sleep_type = mapping.get(r[3], "unknown")
        ts_ns, _u = to_ns_smart(r[0], ctx["now"])
        if ts_ns is None:
            ctx["skips"].note(table, "unplausibler Zeitstempel",
                              {"TIMESTAMP": r[0]})
            continue
        tags = device_tags(ctx["devices"], r[1])
        tags.update({"sample_type": "sleep", "sleep": "state-change"})
        fields = {"intensity": r[2], f"{sleep_type}_sleep": 1}
        ctx["results"].append(make_row(ts_ns, fields, tags))
        observe(ctx, r[1], ts_ns)
        n += 1

        next_ts = r[4]
        if next_ts is None or sleep_type == "waking":
            continue
        end_ns, _ = to_ns_smart(next_ts, ctx["now"])
        if end_ns is None or end_ns <= ts_ns:
            continue
        if (end_ns - ts_ns) / 1e9 / 60 > MAX_EXPAND_MINUTES:
            continue
        t = ts_ns / 1e9
        end_s = end_ns / 1e9
        while t < end_s:
            f2 = {"intensity": r[2], f"{sleep_type}_sleep": 1,
                  "sleep_stage": sleep_type}
            t2 = dict(tags)
            t2["sleep"] = "point-in-time"
            ctx["results"].append(make_row(int(t * 1e9), f2, t2))
            t += 60
    ctx["stats"].note(table, rows=n, unit=label,
                      note="sleep (experimentell)", max_ts_ns=to_ns(max_ts))


### Generischer Catch-All für alle übrigen *_SAMPLE-Tabellen ###

def extract_generic(ctx, handled):
    if not GENERIC_TABLES:
        return
    cur = ctx["cur"]
    for table in get_tables(cur):
        if not (table.endswith("_SAMPLE") or table.endswith("_OVERLAY")):
            continue
        if table in handled:
            continue
        if not table_exists(cur, table):
            continue
        cols = table_columns(cur, table)
        ts_col = next((c for c in TS_CANDIDATES if c in cols), None)
        if not ts_col:
            ctx["stats"].note(table, note="keine Zeitstempel-Spalte - übersprungen")
            continue
        div, label, boundary, max_ts = window_boundary(cur, table, ts_col, ctx["now"])
        col_names = list(cols.keys())
        main_q, main_p, repair_q, repair_p = build_window_queries(
            table, ts_col, boundary, ctx["now"], col_names, label)
        n = skipped = repaired = 0
        blob_cols = set()
        # 1) Reihen laden (primär + Misch-Einheiten-Reparatur) + TEXT-Cardinalität
        rows = []
        text_card = {}
        queries = [(main_q, main_p)]
        if repair_q:
            queries.append((repair_q, repair_p))
        for q, qp in queries:
            for r in cur.execute(q, qp).fetchall():
                d = dict(zip(col_names, r))
                for name, v in d.items():
                    if isinstance(v, str):
                        text_card.setdefault(name, set()).add(v)
                rows.append(d)
        def text_is_tag(name):
            if TEXT_AS_TAG_LIMIT <= 0:
                return False
            card = len(text_card.get(name, ()))
            # kurze WERTE + kleine Cardinalität = Tag; sonst String-Field
            return card > 0 and card <= TEXT_AS_TAG_LIMIT
        for d in rows:
            ts_ns, used_unit = to_ns_smart(d[ts_col], ctx["now"])
            if ts_ns is None:
                ctx["skips"].note(table, "unplausibler Zeitstempel",
                                  {ts_col: d[ts_col]})
                skipped += 1
                continue
            if used_unit and used_unit != label:
                repaired += 1
            fields = {}
            tags = {}
            for name, v in d.items():
                if name == ts_col or v is None:
                    continue
                if name in ("DEVICE_ID", "USER_ID"):
                    continue
                if isinstance(v, bytes):
                    blob_cols.add(name)
                    continue
                if isinstance(v, bool):
                    fields[name.lower()] = v
                elif isinstance(v, (int, float)):
                    if v == -1:  # Gadgetbridge: -1 = kein Wert
                        continue
                    fields[name.lower()] = v
                elif isinstance(v, str):
                    if 0 < len(v) <= 64 and text_is_tag(name):
                        tags[name.lower()] = v
                    elif 0 < len(v) <= 256:
                        fields[name.lower()] = v  # String-Field
                    # sehr lange Werte (z. B. GPX-Pfade) werden übersprungen
            if not fields:
                ctx["skips"].note(table, "alle Felder -1/leer", d)
                continue
            if d.get("DEVICE_ID") is not None:
                tags.update(device_tags(ctx["devices"], d["DEVICE_ID"]))
                observe(ctx, d["DEVICE_ID"], ts_ns)
            tags["sample_type"] = "generic"
            tags["source_table"] = table
            ctx["results"].append(make_row(ts_ns, fields, tags))
            n += 1
        note = ""
        if blob_cols:
            note = f"BLOB-Spalten übersprungen: {', '.join(sorted(blob_cols))}"
        if repaired:
            note = (note + "; " if note else "") + \
                f"{repaired} Zeilen mit reparierter Zeitstempel-Einheit"
        if skipped:
            note = (note + "; " if note else "") + \
                f"{skipped} Zeilen verworfen (ungültiger Zeitstempel)"
        if n == 0 and max_ts is not None:
            note = (note + "; " if note else "") + \
                f"Tabelle HAT Daten, aber KEINE im Abfragefenster! " \
                f"Neueste Zeile: {human(to_ns(max_ts))} UTC"
        ctx["stats"].note(table, rows=n, unit=label, note=note,
                          max_ts_ns=to_ns(max_ts))
        handled.add(table)


### sync_check (Bugfix: Original nutzte veraltete Schleifenvariable r[1]) ###

def add_sync_check(ctx):
    now_ns = time.time_ns()
    for dev_key, last_ts in ctx["devices_observed"].items():
        d = ctx["devices"].get(
            dev_key, {"name": dev_key, "identifier": dev_key, "alias": "Unset"})
        ctx["results"].append(make_row(
            now_ns,
            {"last_seen": last_ts, "last_seen_age": now_ns - last_ts},
            {"device": d["name"], "identifier": d["identifier"],
             "alias": d["alias"], "sample_type": "sync_check"}))
    ctx["stats"].note("sync_check", rows=len(ctx["devices_observed"]))


def check_heart_rate_coverage(cur, ctx):
    """Vergleicht die Anzahl gültiger HR-Zeilen in der DB (Fenster) mit den
    tatsächlich importierten HR-Punkten. Bei 0 importierten HR-Punkten trotz
    vorhandener gültiger HR-Zeilen wird eine deutliche Warnung mit den
    mutmaßlichen Gründen ausgegeben. Kein Crash, nur Diagnose."""
    now = ctx["now"]
    db_hr = {}
    for table in get_tables(cur):
        cols = table_columns(cur, table)
        if "HEART_RATE" not in cols:
            continue
        ts_col = next((c for c in TS_CANDIDATES if c in cols), None)
        if not ts_col:
            continue
        try:
            div, label, boundary, max_ts = window_boundary(cur, table, ts_col, now)
        except Exception:
            continue
        q = (f'SELECT COUNT(*) FROM "{table}" WHERE "{ts_col}" >= ? '
             f'AND "HEART_RATE" IS NOT NULL AND "HEART_RATE" > 20 '
             f'AND "HEART_RATE" < 300')
        c = cur.execute(q, (boundary,)).fetchone()
        if c and c[0] > 0:
            db_hr[table] = c[0]
    if not db_hr:
        return
    imported = sum(1 for row in ctx["results"]
                   if "heart_rate" in row["fields"])
    total_db = sum(db_hr.values())
    if imported == 0 and total_db > 0:
        LOG.warning("*** WARNUNG: %d gültige HR-Zeilen in der DB, aber 0 "
                    "HR-Punkte importiert ***", total_db)
        for t, n in sorted(db_hr.items()):
            LOG.warning("    Tabelle %s: %d Zeilen im Fenster", t, n)
        LOG.warning("    Mögliche Ursachen: Feldname weicht ab, -1-Filter, "
                    "oder die Tabelle wurde von einem anderen Pfad verarbeitet. "
                    "Siehe Skip-Report und Tabellen-Report oben.")
    else:
        LOG.info("HR-Check: %d gültige HR-Zeilen in DB, %d HR-Punkte "
                 "importiert", total_db, imported)


### InfluxDB-Write (mit Batching) ###

def write_results(results):
    if DRY_RUN:
        LOG.info("DRY_RUN: %d Punkte NICHT nach InfluxDB geschrieben", len(results))
        return True
    if not INFLUXDB_URL:
        LOG.error("INFLUXDB_URL ist nicht gesetzt")
        return False
    if InfluxDBClient is None:
        LOG.error("Paket 'influxdb-client' ist nicht installiert "
                  "(pip install influxdb-client)")
        return False

    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN,
                            org=INFLUXDB_ORG)
    written = 0
    failed = 0
    batch = []

    def flush():
        nonlocal written, failed
        if not batch:
            return
        try:
            wapi.write(INFLUXDB_BUCKET, INFLUXDB_ORG, batch)
            written += len(batch)
        except Exception as e:  # noqa: BLE001
            LOG.error("InfluxDB-Write-Fehler (%d Punkte): %s", len(batch), e)
            failed += len(batch)
        batch.clear()

    try:
        with client.write_api() as wapi:
            for row in results:
                fields = {}
                for k, v in row["fields"].items():
                    if v is None:
                        continue
                    # Invalide HR-Werte überspringen (wie im Original)
                    if k == "heart_rate" and isinstance(v, (int, float)) and v > 253:
                        continue
                    fields[k] = v
                if not fields:
                    continue
                p = Point(INFLUXDB_MEASUREMENT)
                for k, v in row["tags"].items():
                    p = p.tag(k, str(v))
                for k, v in fields.items():
                    p = p.field(k, v)
                p = p.time(row["timestamp"])
                batch.append(p)
                if len(batch) >= WRITE_BATCH_SIZE:
                    flush()
            flush()
    except Exception as e:  # noqa: BLE001
        LOG.error("InfluxDB nicht erreichbar (%s): %s", INFLUXDB_URL, e)
        return False

    LOG.info("InfluxDB: %d Punkte geschrieben, %d fehlgeschlagen "
             "(bucket=%s, org=%s)", written, failed, INFLUXDB_BUCKET, INFLUXDB_ORG)
    return failed == 0


### Hauptprogramm ###

def main():
    tempdir = None
    try:
        # 1) DB beziehen (WebDAV oder lokal)
        if LOCAL_DB:
            db_path = LOCAL_DB
            if not os.path.exists(db_path):
                LOG.error("LOCAL_DB-Datei nicht gefunden: %s", db_path)
                sys.exit(1)
            LOG.info("Nutze lokale DB: %s", db_path)
        else:
            if not WEBDAV_URL:
                LOG.error("WEBDAV_URL oder LOCAL_DB muss gesetzt sein")
                sys.exit(1)
            if WebDavClient is None:
                LOG.error("Paket 'webdavclient3' ist nicht installiert "
                          "(pip install webdavclient3)")
                sys.exit(1)
            LOG.info("Lade Export '%s' von WebDAV ...", EXPORT_FILE)
            webdav_client = WebDavClient({
                "webdav_hostname": WEBDAV_URL,
                "webdav_login": WEBDAV_USER,
                "webdav_password": WEBDAV_PASS,
            })
            file_list = webdav_client.list(WEBDAV_PATH)
            if EXPORT_FILE not in file_list:
                LOG.error("Export-Datei nicht gefunden unter %s "
                          "(vorhanden: %s)", WEBDAV_PATH, file_list)
                sys.exit(1)
            tempdir = tempfile.mkdtemp()
            db_path = f"{tempdir}/gadgetbridge.sqlite"
            webdav_client.download_sync(
                remote_path=f"{WEBDAV_PATH}/{EXPORT_FILE}", local_path=db_path)
            LOG.info("Geladen: %s", db_path)

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        now = int(time.time())
        window_start = now - QUERY_DURATION
        LOG.info("Abfragefenster: %s UTC -> jetzt (QUERY_DURATION=%ds)",
                 time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(window_start)),
                 QUERY_DURATION)

        # 2) Geräte
        devices = get_devices(cur)
        if devices:
            LOG.info("Geräte im Export: %s",
                     [f"{d['name']} ({d['identifier']})" for d in devices.values()])
        else:
            LOG.warning("Keine Geräte in der DEVICE-Tabelle - "
                        "stimmt der Export (richtes Konto/Gerät)?")

        stats = Stats()
        skips = Skips()
        ctx = ctx_make(cur, devices, now, stats, skips)
        handled = set()

        # 3) bekannte Tabellen (alle Hersteller)
        for table, spec in ALL_SPECS.items():
            if run_spec(ctx, table, spec):
                handled.add(table)

        # 4) Sonderlogik
        extract_activity_tables(ctx)
        handled.update(("MI_BAND_ACTIVITY_SAMPLE",
                        "HUAMI_EXTENDED_ACTIVITY_SAMPLE"))
        extract_huami_stress(ctx)
        handled.add("HUAMI_STRESS_SAMPLE")
        if "SLEEP" in EXPERIMENTAL_OPTS:
            LOG.info("Experimentelle Schlafdaten (Mi-Band RAW_KINDs) aktiv")
            get_sleep_data(ctx)
        else:
            ctx["stats"].note("HUAMI/MI sleep", note="inaktiv (EXPERIMENTAL_OPTS=SLEEP fehlt)")

        # 5) generischer Catch-All
        extract_generic(ctx, handled)

        # 6) sync_check
        add_sync_check(ctx)

        stats.report()
        skips.report()

        # HR-Plausibilitäts-Check: Wenn die DB gültige HEART_RATE-Werte im
        # Fenster enthält, aber wir keinen einzigen HR-Punkt geschrieben haben,
        # gibt es ein echtes Problem - jetzt wird es sichtbar statt still.
        check_heart_rate_coverage(cur, ctx)

        if not ctx["results"]:
            LOG.error("Es konnte keine einzige Zeile extrahiert werden. "
                      "Siehe Tabellen-Report oben: Welche Tabellen haben Daten?")
            conn.close()
            sys.exit(1)

        LOG.info("Insgesamt %d Punkte extrahiert", len(ctx["results"]))
        ok = write_results(ctx["results"])
        conn.close()
        if not ok:
            sys.exit(1)
    finally:
        if tempdir and tempdir not in ["/", ""]:
            if REMOVE_TEMP_DB == "N":
                LOG.info("Temporäre DB behalten: %s", tempdir)
            else:
                shutil.rmtree(tempdir, ignore_errors=True)


if __name__ == "__main__":
    main()
