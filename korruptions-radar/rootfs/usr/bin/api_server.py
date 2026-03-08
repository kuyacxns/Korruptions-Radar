#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
import sqlite3, json, os
from pathlib import Path

DB_PATH = Path("/data/korruptions_radar/korruptions_radar.db")

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(format % args)

    def send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path):
        with open(path, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        p = self.path.split("?")[0]
        try:
            if p == "/" or p == "/dashboard":
                self.send_file("/usr/share/korruptions_radar/index.html")
            elif p == "/api/zusammenfassung":
                c = db().cursor()
                self.send_json({
                    "total_spenden": c.execute("SELECT COALESCE(SUM(betrag_eur),0) FROM parteispenden").fetchone()[0],
                    "anzahl_spenden": c.execute("SELECT COUNT(*) FROM parteispenden").fetchone()[0],
                    "anzahl_abstimmungen": c.execute("SELECT COUNT(*) FROM abstimmungen").fetchone()[0],
                    "verdaechtige_korrelationen": c.execute("SELECT COUNT(*) FROM korrelationen WHERE verdachts_score>=0.5").fetchone()[0],
                    "anzahl_nebeneinkuenfte": c.execute("SELECT COUNT(*) FROM nebeneinkuenfte").fetchone()[0],
                    "super_score_faelle": c.execute("SELECT COUNT(*) FROM super_korrelationen WHERE super_score>=0.6").fetchone()[0],
                })
            elif p == "/api/korrelationen":
                c = db().cursor()
                rows = c.execute("""
                    SELECT k.verdachts_score, k.tage_abstand, k.begruendung,
                           p.spender, p.empfaenger AS partei, p.betrag_eur,
                           p.datum AS spenden_datum, p.branche,
                           a.titel AS abstimmung, a.datum AS abst_datum, a.thema
                    FROM korrelationen k
                    JOIN parteispenden p ON k.spende_id=p.id
                    JOIN abstimmungen a ON k.abstimmung_id=a.id
                    ORDER BY k.verdachts_score DESC LIMIT 50
                """).fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/spenden_partei":
                c = db().cursor()
                rows = c.execute("SELECT empfaenger AS partei, SUM(betrag_eur) AS gesamt FROM parteispenden GROUP BY empfaenger ORDER BY gesamt DESC").fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/spenden_branche":
                c = db().cursor()
                rows = c.execute("SELECT branche, SUM(betrag_eur) AS gesamt FROM parteispenden GROUP BY branche ORDER BY gesamt DESC").fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/alle_spenden":
                c = db().cursor()
                rows = c.execute("SELECT * FROM parteispenden ORDER BY datum DESC").fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/nebeneinkuenfte":
                c = db().cursor()
                rows = c.execute("SELECT ne.*, a.name, a.partei FROM nebeneinkuenfte ne LEFT JOIN abgeordnete a ON ne.abgeordneter_id=a.id ORDER BY ne.betrag_min DESC LIMIT 100").fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/drehtuer":
                c = db().cursor()
                rows = c.execute("SELECT * FROM drehtuer ORDER BY verdachts_score DESC LIMIT 50").fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/api/super_scores":
                c = db().cursor()
                rows = c.execute("""
                    SELECT sk.super_score, sk.faktoren, a.name, a.partei,
                           au.ausschuss_name AS ausschuss, ne.organisation,
                           p.spender, p.betrag_eur, abst.titel AS abstimmung
                    FROM super_korrelationen sk
                    LEFT JOIN abgeordnete a ON sk.abgeordneter_id=a.id
                    LEFT JOIN ausschuesse au ON sk.ausschuss_id=au.id
                    LEFT JOIN nebeneinkuenfte ne ON sk.nebeneinkuenfte_id=ne.id
                    LEFT JOIN parteispenden p ON sk.spende_id=p.id
                    LEFT JOIN abstimmungen abst ON sk.abstimmung_id=abst.id
                    ORDER BY sk.super_score DESC LIMIT 30
                """).fetchall()
                self.send_json([dict(r) for r in rows])
            elif p == "/neu_analysieren":
                os.system("python3 /usr/bin/daten_sammler.py --demo &")
                self.send_json({"status": "gestartet"})
            else:
                self.send_json({"error": "not found"}, 404)
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 7755), Handler)
    print("Server läuft auf Port 7755")
    server.serve_forever()
