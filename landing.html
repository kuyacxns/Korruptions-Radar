"""
Korruptions-Radar API Server
Läuft auf Port 7755, stellt Daten für das Dashboard bereit
"""

from flask import Flask, jsonify, send_from_directory
import sqlite3
import json
import os
import sys

# Reporter-Tools einbinden
sys.path.insert(0, '/usr/bin')
try:
    from reporter_tools import register_reporter_routes
    HAS_REPORTER = True
except ImportError:
    HAS_REPORTER = False

app = Flask(__name__, static_folder="/usr/share/korruptions_radar")

DB_PATH = "/data/korruptions_radar/korruptions_radar.db"

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── API Endpunkte ─────────────────────────────────────────────────────────────

@app.route("/api/zusammenfassung")
def zusammenfassung():
    c = db().cursor()
    return jsonify({
        "total_spenden":              c.execute("SELECT COALESCE(SUM(betrag_eur),0) FROM parteispenden").fetchone()[0],
        "anzahl_spenden":             c.execute("SELECT COUNT(*) FROM parteispenden").fetchone()[0],
        "anzahl_abstimmungen":        c.execute("SELECT COUNT(*) FROM abstimmungen").fetchone()[0],
        "verdaechtige_korrelationen": c.execute("SELECT COUNT(*) FROM korrelationen WHERE verdachts_score >= 0.5").fetchone()[0],
        "anzahl_nebeneinkuenfte":     c.execute("SELECT COUNT(*) FROM nebeneinkuenfte").fetchone()[0],
        "drehtuer_faelle":            c.execute("SELECT COUNT(*) FROM drehtuer").fetchone()[0],
        "super_score_faelle":         c.execute("SELECT COUNT(*) FROM super_korrelationen WHERE super_score >= 0.6").fetchone()[0],
    })

@app.route("/api/korrelationen")
def korrelationen():
    c = db().cursor()
    rows = c.execute("""
        SELECT k.verdachts_score, k.tage_abstand, k.begruendung,
               p.spender, p.empfaenger AS partei, p.betrag_eur,
               p.datum AS spenden_datum, p.branche,
               a.titel AS abstimmung, a.datum AS abstimmungs_datum, a.thema
        FROM korrelationen k
        JOIN parteispenden p ON k.spende_id = p.id
        JOIN abstimmungen a  ON k.abstimmung_id = a.id
        ORDER BY k.verdachts_score DESC
        LIMIT 50
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/spenden_partei")
def spenden_partei():
    c = db().cursor()
    rows = c.execute("""
        SELECT empfaenger AS partei, SUM(betrag_eur) AS gesamt, COUNT(*) AS anzahl
        FROM parteispenden GROUP BY empfaenger ORDER BY gesamt DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/spenden_branche")
def spenden_branche():
    c = db().cursor()
    rows = c.execute("""
        SELECT branche, SUM(betrag_eur) AS gesamt
        FROM parteispenden GROUP BY branche ORDER BY gesamt DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/alle_spenden")
def alle_spenden():
    c = db().cursor()
    rows = c.execute("SELECT * FROM parteispenden ORDER BY datum DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/abstimmungen")
def abstimmungen():
    c = db().cursor()
    rows = c.execute("SELECT * FROM abstimmungen ORDER BY datum DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/nebeneinkuenfte")
def nebeneinkuenfte():
    c = db().cursor()
    rows = c.execute("""
        SELECT ne.*, a.name, a.partei
        FROM nebeneinkuenfte ne
        LEFT JOIN abgeordnete a ON ne.abgeordneter_id = a.id
        ORDER BY ne.betrag_min DESC
        LIMIT 100
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/drehtuer")
def drehtuer():
    c = db().cursor()
    rows = c.execute("""
        SELECT * FROM drehtuer ORDER BY verdachts_score DESC LIMIT 50
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/super_scores")
def super_scores():
    c = db().cursor()
    rows = c.execute("""
        SELECT sk.*, a.name, a.partei,
               au.ausschuss_name AS ausschuss,
               ne.organisation, ne.branche AS ne_branche,
               p.spender, p.betrag_eur, p.datum AS spenden_datum,
               abst.titel AS abstimmung, abst.datum AS abstimmungs_datum
        FROM super_korrelationen sk
        LEFT JOIN abgeordnete a  ON sk.abgeordneter_id = a.id
        LEFT JOIN ausschuesse au ON sk.ausschuss_id = au.id
        LEFT JOIN nebeneinkuenfte ne ON sk.nebeneinkuenfte_id = ne.id
        LEFT JOIN parteispenden p    ON sk.spende_id = p.id
        LEFT JOIN abstimmungen abst  ON sk.abstimmung_id = abst.id
        ORDER BY sk.super_score DESC
        LIMIT 30
    """).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/neu_analysieren", methods=["POST"])
def neu_analysieren():
    """Startet die Korrelationsanalyse neu"""
    import subprocess
    subprocess.Popen(["python3", "/usr/bin/daten_sammler.py"])
    return jsonify({"status": "Analyse gestartet"})

@app.route("/neu_analysieren", methods=["POST"])
def neu_analysieren_redirect():
    return neu_analysieren()

# Reporter-Routen registrieren (Export, Report, Landing)
if HAS_REPORTER:
    register_reporter_routes(app)
else:
    # Fallback Landing Page
    @app.route("/")
    def landing_fallback():
        return send_from_directory("/usr/share/korruptions_radar", "landing.html")

    @app.route("/dashboard")
    def dashboard_fallback():
        return send_from_directory("/usr/share/korruptions_radar", "index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7755, debug=False)
