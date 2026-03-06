"""
==============================================================
KORRUPTIONS-RADAR: Reporter-Tools
==============================================================
Generiert:
  1. Wöchentlichen Markdown/HTML Bericht
  2. CSV-Export für Journalisten
  3. JSON-API Erweiterungen

Starten:
  python3 reporter_tools.py --report   → Wochenbericht generieren
  python3 reporter_tools.py --csv      → CSV exportieren
  python3 reporter_tools.py --weekly   → Wochenbericht + Email/Webhook
==============================================================
"""

import sqlite3, csv, json, datetime, sys, os, urllib.request
from pathlib import Path

DB_PATH     = Path("/data/korruptions_radar/korruptions_radar.db")
REPORTS_DIR = Path("/data/korruptions_radar/reports")

# ── Optional: Webhook für ntfy.sh oder ähnliches
# WEBHOOK_URL = "https://ntfy.sh/dein-kanal"
WEBHOOK_URL = None


# ═══════════════════════════════════════════════════════════
# DATENBANK HELPER
# ═══════════════════════════════════════════════════════════

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fmt_eur(n):
    if not n:
        return "—"
    return f"{n:,.0f} €".replace(",", ".")


def fmt_datum(d):
    if not d:
        return "—"
    try:
        dt = datetime.date.fromisoformat(d[:10])
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return d


# ═══════════════════════════════════════════════════════════
# CSV EXPORT
# ═══════════════════════════════════════════════════════════

def exportiere_csv():
    """
    Erstellt mehrere CSV-Dateien:
    - korrelationen.csv     (Haupt-Analyseergebnisse)
    - parteispenden.csv     (Rohdaten Spenden)
    - nebeneinkuenfte.csv   (Rohdaten Nebeneinkünfte)
    - drehtuer.csv          (Drehtür-Analyse)
    - super_scores.csv      (Super-Score Fälle)
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = db()
    exportiert = []

    # 1. Korrelationen
    rows = conn.execute("""
        SELECT
            k.verdachts_score AS score,
            k.tage_abstand,
            k.begruendung,
            p.spender,
            p.empfaenger AS partei,
            p.betrag_eur,
            p.datum AS spenden_datum,
            p.branche,
            a.titel AS abstimmung,
            a.datum AS abstimmungs_datum,
            a.thema,
            p.quelle_url AS quelle
        FROM korrelationen k
        JOIN parteispenden p ON k.spende_id = p.id
        JOIN abstimmungen a  ON k.abstimmung_id = a.id
        ORDER BY k.verdachts_score DESC
    """).fetchall()

    pfad = REPORTS_DIR / "korrelationen.csv"
    with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=[
            "score","tage_abstand","spender","partei","betrag_eur",
            "spenden_datum","branche","abstimmung","abstimmungs_datum","thema","begruendung","quelle"
        ])
        w.writeheader()
        w.writerows([dict(r) for r in rows])
    exportiert.append(("korrelationen.csv", len(rows)))

    # 2. Parteispenden
    rows = conn.execute("SELECT * FROM parteispenden ORDER BY datum DESC").fetchall()
    pfad = REPORTS_DIR / "parteispenden.csv"
    with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows([dict(r) for r in rows])
    exportiert.append(("parteispenden.csv", len(rows)))

    # 3. Nebeneinkünfte
    rows = conn.execute("""
        SELECT ne.*, a.name AS abgeordneter, a.partei
        FROM nebeneinkuenfte ne
        LEFT JOIN abgeordnete a ON ne.abgeordneter_id = a.id
        ORDER BY ne.betrag_min DESC
    """).fetchall()
    pfad = REPORTS_DIR / "nebeneinkuenfte.csv"
    with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows([dict(r) for r in rows])
    exportiert.append(("nebeneinkuenfte.csv", len(rows)))

    # 4. Drehtür
    rows = conn.execute("SELECT * FROM drehtuer ORDER BY verdachts_score DESC").fetchall()
    pfad = REPORTS_DIR / "drehtuer.csv"
    with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows([dict(r) for r in rows])
    exportiert.append(("drehtuer.csv", len(rows)))

    # 5. Super-Scores
    rows = conn.execute("""
        SELECT sk.super_score, sk.faktoren,
               a.name, a.partei,
               au.ausschuss_name AS ausschuss, au.rolle,
               ne.organisation, ne.branche, ne.betrag_min, ne.betrag_max,
               p.spender, p.betrag_eur AS spenden_betrag,
               p.datum AS spenden_datum,
               abst.titel AS abstimmung, abst.datum AS abstimmungs_datum
        FROM super_korrelationen sk
        LEFT JOIN abgeordnete a     ON sk.abgeordneter_id = a.id
        LEFT JOIN ausschuesse au    ON sk.ausschuss_id = au.id
        LEFT JOIN nebeneinkuenfte ne ON sk.nebeneinkuenfte_id = ne.id
        LEFT JOIN parteispenden p    ON sk.spende_id = p.id
        LEFT JOIN abstimmungen abst  ON sk.abstimmung_id = abst.id
        ORDER BY sk.super_score DESC
    """).fetchall()
    pfad = REPORTS_DIR / "super_scores.csv"
    with open(pfad, "w", newline="", encoding="utf-8-sig") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows([dict(r) for r in rows])
    exportiert.append(("super_scores.csv", len(rows)))

    conn.close()
    print(f"✓ CSV-Export abgeschlossen:")
    for name, n in exportiert:
        print(f"  {REPORTS_DIR}/{name} ({n} Zeilen)")
    return exportiert


# ═══════════════════════════════════════════════════════════
# WOCHENBERICHT
# ═══════════════════════════════════════════════════════════

def generiere_wochenbericht():
    """
    Erstellt einen strukturierten Wochenbericht als Markdown.
    Ideal für Journalisten und NGOs.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = db()
    heute = datetime.date.today()
    kw    = heute.isocalendar()[1]
    jahr  = heute.year

    # ── Daten laden
    zus = dict(conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM parteispenden) AS spenden,
            (SELECT COALESCE(SUM(betrag_eur),0) FROM parteispenden) AS spenden_gesamt,
            (SELECT COUNT(*) FROM abstimmungen) AS abstimmungen,
            (SELECT COUNT(*) FROM korrelationen WHERE verdachts_score >= 0.5) AS korrelationen,
            (SELECT COUNT(*) FROM nebeneinkuenfte) AS nebeneinkuenfte,
            (SELECT COUNT(*) FROM drehtuer) AS drehtuer,
            (SELECT COUNT(*) FROM super_korrelationen WHERE super_score >= 0.6) AS super_scores
    """).fetchone())

    top_korr = conn.execute("""
        SELECT k.verdachts_score, k.tage_abstand, k.begruendung,
               p.spender, p.empfaenger AS partei, p.betrag_eur,
               p.datum AS spenden_datum, p.branche,
               a.titel, a.datum AS abst_datum, a.thema
        FROM korrelationen k
        JOIN parteispenden p ON k.spende_id = p.id
        JOIN abstimmungen a  ON k.abstimmung_id = a.id
        ORDER BY k.verdachts_score DESC LIMIT 10
    """).fetchall()

    top_drehtuer = conn.execute("""
        SELECT * FROM drehtuer ORDER BY verdachts_score DESC LIMIT 5
    """).fetchall()

    top_super = conn.execute("""
        SELECT sk.super_score, sk.faktoren,
               a.name, a.partei,
               au.ausschuss_name AS ausschuss,
               ne.organisation, ne.betrag_min,
               p.spender, p.betrag_eur,
               abst.titel AS abstimmung
        FROM super_korrelationen sk
        LEFT JOIN abgeordnete a      ON sk.abgeordneter_id = a.id
        LEFT JOIN ausschuesse au     ON sk.ausschuss_id = au.id
        LEFT JOIN nebeneinkuenfte ne ON sk.nebeneinkuenfte_id = ne.id
        LEFT JOIN parteispenden p    ON sk.spende_id = p.id
        LEFT JOIN abstimmungen abst  ON sk.abstimmung_id = abst.id
        ORDER BY sk.super_score DESC LIMIT 5
    """).fetchall()

    conn.close()

    # ── Bericht zusammenstellen
    score_stern = lambda s: "🔴" if s>=0.8 else "🟠" if s>=0.6 else "🟡"

    bericht = f"""# Korruptions-Radar — Wochenbericht KW {kw}/{jahr}
*Automatisch generiert am {heute.strftime("%d.%m.%Y")} | Alle Daten aus öffentlichen Quellen*

---

## 📊 Zusammenfassung

| Kennzahl | Wert |
|---|---|
| Erfasste Großspenden | {zus['spenden']} |
| Gesamtvolumen Spenden | {fmt_eur(zus['spenden_gesamt'])} |
| Analysierte Abstimmungen | {zus['abstimmungen']} |
| Verdächtige Korrelationen (Score ≥0.5) | **{zus['korrelationen']}** |
| Erfasste Nebeneinkünfte | {zus['nebeneinkuenfte']} |
| Drehtür-Verdachtsfälle | {zus['drehtuer']} |
| Super-Score Fälle (Score ≥0.6) | **{zus['super_scores']}** |

---

## 🔴 Top 10 Verdächtige Korrelationen

*Score 0–100: Wie auffällig ist das zeitliche Zusammenfallen von Spende und Abstimmung?*

"""
    for i, k in enumerate(top_korr, 1):
        bericht += f"""### {i}. {score_stern(k['verdachts_score'])} Score {int(k['verdachts_score']*100)}/100 — {k['spender']} → {k['partei']}

- **Spende:** {fmt_eur(k['betrag_eur'])} am {fmt_datum(k['spenden_datum'])}
- **Branche:** {k['branche']}
- **Abstimmung:** „{k['titel']}" am {fmt_datum(k['abst_datum'])}
- **Zeitabstand:** {k['tage_abstand']} Tage
- **Thema:** {k['thema']}
- **Verdachtsgründe:** {k['begruendung']}

"""

    if top_drehtuer:
        bericht += f"""---

## 🔄 Drehtür-Verdachtsfälle (Top 5)

*Abgeordnete die gleichzeitig in einem Ausschuss sitzen UND Nebeneinkünfte aus der gleichen Branche beziehen*

"""
        for d in top_drehtuer:
            bericht += f"""- {score_stern(d['verdachts_score'])} **{d['name']}** ({d['partei']})
  - Ausschuss: {d['position_vorher']}
  - Nebeneinkünfte bei: {d['organisation_nachher']} ({d['branche']})
  - Gründe: {d['begruendung']}

"""

    if top_super:
        bericht += f"""---

## 🎯 Super-Score Fälle (Top 5)

*Kombination aus: Spende + Nebeneinkünfte + Ausschuss + Abstimmung*

"""
        for s in top_super:
            faktoren = {}
            try:
                faktoren = json.loads(s['faktoren'] or '{}')
            except Exception:
                pass
            bericht += f"""- 🎯 **Score {int(s['super_score']*100)}/100** — **{s['name']}** ({s['partei']})
  - Ausschuss: {s['ausschuss'] or '—'}
  - Nebeneinkünfte bei: {s['organisation'] or '—'}
  - Parteispende: {s['spender'] or '—'} ({fmt_eur(s['betrag_eur'])})
  - Abstimmung: {s['abstimmung'] or '—'}
  - Score-Faktoren: {', '.join(f"{k}=+{int(v*100)}" for k,v in faktoren.items())}

"""

    bericht += f"""---

## ⚠️ Wichtige Hinweise zur Interpretation

1. **Korrelation ≠ Kausalität** — Ein hoher Score bedeutet nicht automatisch, dass Korruption stattgefunden hat
2. **Ausgangspunkt für Recherche** — Die Ergebnisse sind als Hinweis für weitere journalistische Recherche gedacht
3. **Öffentliche Quellen** — Alle Daten stammen aus gesetzlich vorgeschriebenen Pflichtmeldungen
4. **Zeitverzögerung** — Kleinere Spenden werden erst mit bis zu einem Jahr Verzögerung veröffentlicht

## 📥 Rohdaten

Alle Daten als CSV: `/api/export/csv`
Live-Dashboard: `/dashboard`
API-Dokumentation: `/api/zusammenfassung`

---
*Korruptions-Radar Deutschland · Öffentliches Transparenzprojekt*
*Quellen: bundestag.de · abgeordnetenwatch.de · lobbyregister.bundestag.de*
"""

    # Speichern
    md_pfad = REPORTS_DIR / f"wochenbericht_kw{kw}_{jahr}.md"
    with open(md_pfad, "w", encoding="utf-8") as f:
        f.write(bericht)
    print(f"✓ Wochenbericht gespeichert: {md_pfad}")

    # Auch als aktuelle Version speichern
    aktuell_pfad = REPORTS_DIR / "wochenbericht_aktuell.md"
    with open(aktuell_pfad, "w", encoding="utf-8") as f:
        f.write(bericht)

    return bericht, md_pfad


# ═══════════════════════════════════════════════════════════
# WEBHOOK / PUSH-BENACHRICHTIGUNG
# ═══════════════════════════════════════════════════════════

def sende_webhook(bericht_kurz):
    """
    Sendet eine Benachrichtigung via ntfy.sh (kostenlos, kein Account nötig).
    Einrichten: WEBHOOK_URL = "https://ntfy.sh/dein-geheimer-kanal"
    """
    if not WEBHOOK_URL:
        print("  ℹ Kein Webhook konfiguriert (WEBHOOK_URL in reporter_tools.py setzen)")
        return

    try:
        nachricht = bericht_kurz[:500].encode("utf-8")
        req = urllib.request.Request(
            WEBHOOK_URL,
            data=nachricht,
            headers={
                "Title": "Korruptions-Radar Wochenbericht",
                "Priority": "high",
                "Tags": "rotating_light,chart_with_upwards_trend",
                "Content-Type": "text/plain; charset=utf-8",
            }
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"  ✓ Webhook gesendet: HTTP {r.status}")
    except Exception as e:
        print(f"  ⚠ Webhook fehlgeschlagen: {e}")


# ═══════════════════════════════════════════════════════════
# FLASK ROUTEN (werden in api_server.py eingebunden)
# ═══════════════════════════════════════════════════════════

def register_reporter_routes(app):
    """Registriert zusätzliche Routen im Flask-Server."""
    from flask import send_file, jsonify, Response
    import io

    @app.route("/api/export/csv")
    def export_csv_all():
        """Gibt alle Korrelationen als CSV zurück."""
        conn = db()
        rows = conn.execute("""
            SELECT k.verdachts_score AS score,
                   k.tage_abstand, k.begruendung,
                   p.spender, p.empfaenger AS partei,
                   p.betrag_eur, p.datum AS spenden_datum, p.branche,
                   a.titel AS abstimmung, a.datum AS abstimmungs_datum, a.thema
            FROM korrelationen k
            JOIN parteispenden p ON k.spende_id = p.id
            JOIN abstimmungen a  ON k.abstimmung_id = a.id
            ORDER BY k.verdachts_score DESC
        """).fetchall()
        conn.close()

        output = io.StringIO()
        output.write('\ufeff')  # BOM für Excel
        w = csv.DictWriter(output, fieldnames=[
            "score","tage_abstand","spender","partei","betrag_eur",
            "spenden_datum","branche","abstimmung","abstimmungs_datum","thema","begruendung"
        ])
        w.writeheader()
        w.writerows([dict(r) for r in rows])

        heute = datetime.date.today().strftime("%Y-%m-%d")
        return Response(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename=korruptions_radar_{heute}.csv"}
        )

    @app.route("/api/export/report")
    def export_report():
        """Gibt den aktuellen Wochenbericht als Markdown zurück."""
        aktuell = REPORTS_DIR / "wochenbericht_aktuell.md"
        if not aktuell.exists():
            bericht, _ = generiere_wochenbericht()
            return Response(bericht, mimetype="text/markdown; charset=utf-8",
                headers={"Content-Disposition": "attachment; filename=korruptions_radar_report.md"})
        with open(aktuell, encoding="utf-8") as f:
            inhalt = f.read()
        return Response(inhalt, mimetype="text/markdown; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=korruptions_radar_report.md"})

    @app.route("/api/export/json")
    def export_json_full():
        """Vollständiger JSON-Export aller Daten."""
        conn = db()
        data = {
            "generiert_am": datetime.datetime.now().isoformat(),
            "version": "2.0",
            "quellen": ["bundestag.de", "abgeordnetenwatch.de", "lobbyregister.bundestag.de"],
            "korrelationen": [dict(r) for r in conn.execute("""
                SELECT k.verdachts_score, k.tage_abstand, k.begruendung,
                       p.spender, p.empfaenger AS partei, p.betrag_eur,
                       p.datum AS spenden_datum, p.branche,
                       a.titel, a.datum AS abst_datum, a.thema
                FROM korrelationen k
                JOIN parteispenden p ON k.spende_id = p.id
                JOIN abstimmungen a  ON k.abstimmung_id = a.id
                ORDER BY k.verdachts_score DESC
            """).fetchall()],
            "drehtuer": [dict(r) for r in conn.execute("SELECT * FROM drehtuer ORDER BY verdachts_score DESC").fetchall()],
            "super_scores": [dict(r) for r in conn.execute("""
                SELECT sk.super_score, a.name, a.partei,
                       au.ausschuss_name, ne.organisation, p.spender, p.betrag_eur,
                       abst.titel AS abstimmung
                FROM super_korrelationen sk
                LEFT JOIN abgeordnete a     ON sk.abgeordneter_id = a.id
                LEFT JOIN ausschuesse au    ON sk.ausschuss_id = au.id
                LEFT JOIN nebeneinkuenfte ne ON sk.nebeneinkuenfte_id = ne.id
                LEFT JOIN parteispenden p   ON sk.spende_id = p.id
                LEFT JOIN abstimmungen abst ON sk.abstimmung_id = abst.id
                ORDER BY sk.super_score DESC
            """).fetchall()],
        }
        conn.close()
        return jsonify(data)

    @app.route("/")
    def landing():
        """Öffentliche Landing Page."""
        from flask import send_from_directory
        return send_from_directory("/usr/share/korruptions_radar", "landing.html")

    @app.route("/dashboard")
    def dashboard():
        """Internes Dashboard."""
        from flask import send_from_directory
        return send_from_directory("/usr/share/korruptions_radar", "index.html")


# ═══════════════════════════════════════════════════════════
# HAUPTPROGRAMM
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    modus = sys.argv[1] if len(sys.argv) > 1 else "--report"

    if modus == "--csv":
        exportiere_csv()

    elif modus == "--weekly":
        print("📰 Generiere Wochenbericht...")
        bericht, pfad = generiere_wochenbericht()
        exportiere_csv()
        # Kurzzusammenfassung für Webhook
        zeilen = bericht.split('\n')
        kurz = '\n'.join(zeilen[:20])
        sende_webhook(kurz)
        print(f"\n✅ Wochenbericht fertig: {pfad}")

    else:  # --report
        print("📰 Generiere Bericht...")
        bericht, pfad = generiere_wochenbericht()
        print(f"\n✅ Bericht: {pfad}")
        print("\n── VORSCHAU ──────────────────────────")
        print('\n'.join(bericht.split('\n')[:30]))
        print("...")
