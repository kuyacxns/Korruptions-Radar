"""
==============================================================
KORRUPTIONS-RADAR v2: Erweiterter Datensammler
==============================================================
Datenquellen:
  1. Parteispenden        → bundestag.api.proxy.bund.dev
  2. Abgeordnete          → abgeordnetenwatch.de/api/v2
  3. Abstimmungen         → abgeordnetenwatch.de/api/v2
  4. Abstimmungsergebnisse→ abgeordnetenwatch.de/api/v2
  5. NEU: Nebeneinkünfte  → abgeordnetenwatch.de/api/v2/sidejobs
  6. NEU: Ausschüsse      → bundestag.de/xml/v2/ausschuesse
  7. NEU: Drehtür-Effekt  → Wechsel Politik ↔ Wirtschaft (berechnet)

Starten:
  python3 daten_sammler.py          → alles laden & analysieren
  python3 daten_sammler.py --demo   → Demo-Daten (offline)
  python3 daten_sammler.py --test   → API-Test
==============================================================
"""

import sqlite3, json, random, datetime, time, sys, re
import urllib.request, urllib.parse
from pathlib import Path

DB_PATH   = Path("/data/korruptions_radar/korruptions_radar.db")
CACHE_DIR = Path("/data/korruptions_radar/cache")

DIP_API_KEY    = "OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw"
WAHLPERIODE_ID = 161   # 21. Bundestag 2025-2029
RATE_LIMIT     = 0.5   # Sekunden zwischen Anfragen


# ═══════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ═══════════════════════════════════════════════════════════

def api_get(url, headers=None, cache_key=None, cache_h=12):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if cache_key:
        cf = CACHE_DIR / f"{cache_key}.json"
        if cf.exists() and (time.time() - cf.stat().st_mtime) / 3600 < cache_h:
            with open(cf, encoding="utf-8") as f:
                return json.load(f)
    req = urllib.request.Request(url, headers=headers or {})
    req.add_header("User-Agent", "KorruptionsRadar/2.0 (Bildungsprojekt)")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        if cache_key:
            with open(CACHE_DIR / f"{cache_key}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        time.sleep(RATE_LIMIT)
        return data
    except Exception as e:
        print(f"  ⚠ API: {url[:60]}… → {e}")
        return None


def xml_get(url, cache_key=None):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if cache_key:
        cf = CACHE_DIR / f"{cache_key}.xml"
        if cf.exists() and (time.time() - cf.stat().st_mtime) / 3600 < 24:
            with open(cf, encoding="utf-8") as f:
                return f.read()
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "KorruptionsRadar/2.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            xml = r.read().decode("utf-8", errors="replace")
        if cache_key:
            with open(CACHE_DIR / f"{cache_key}.xml", "w", encoding="utf-8") as f:
                f.write(xml)
        time.sleep(RATE_LIMIT)
        return xml
    except Exception as e:
        print(f"  ⚠ XML: {url[:60]}… → {e}")
        return None


def html_get(url, cache_key=None, cache_h=24):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if cache_key:
        cf = CACHE_DIR / f"{cache_key}.html"
        if cf.exists() and (time.time() - cf.stat().st_mtime) / 3600 < cache_h:
            with open(cf, encoding="utf-8") as f:
                return f.read()
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "KorruptionsRadar/2.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        if cache_key:
            with open(CACHE_DIR / f"{cache_key}.html", "w", encoding="utf-8") as f:
                f.write(html)
        time.sleep(RATE_LIMIT)
        return html
    except Exception as e:
        print(f"  ⚠ HTML: {url[:60]}… → {e}")
        return None


# ═══════════════════════════════════════════════════════════
# DATENBANK
# ═══════════════════════════════════════════════════════════

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        -- Bestehende Tabellen
        CREATE TABLE IF NOT EXISTS abgeordnete (
            id INTEGER PRIMARY KEY, aw_id INTEGER UNIQUE,
            name TEXT NOT NULL, partei TEXT, wahlkreis TEXT, aktiv INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS parteispenden (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spender TEXT NOT NULL, empfaenger TEXT NOT NULL,
            betrag_eur REAL NOT NULL, datum TEXT NOT NULL,
            branche TEXT, quelle_url TEXT, quelle TEXT DEFAULT 'bundestag'
        );
        CREATE TABLE IF NOT EXISTS abstimmungen (
            id INTEGER PRIMARY KEY, aw_id INTEGER UNIQUE,
            titel TEXT NOT NULL, datum TEXT NOT NULL,
            thema TEXT, beschreibung TEXT
        );
        CREATE TABLE IF NOT EXISTS abstimmungs_ergebnis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            abstimmung_id INTEGER REFERENCES abstimmungen(id),
            abgeordneter_id INTEGER REFERENCES abgeordnete(id),
            votum TEXT, partei TEXT
        );
        CREATE TABLE IF NOT EXISTS korrelationen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spende_id INTEGER REFERENCES parteispenden(id),
            abstimmung_id INTEGER REFERENCES abstimmungen(id),
            tage_abstand INTEGER, verdachts_score REAL,
            begruendung TEXT, erstellt_am TEXT DEFAULT CURRENT_TIMESTAMP
        );

        -- NEU: Nebeneinkünfte
        CREATE TABLE IF NOT EXISTS nebeneinkuenfte (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            aw_id INTEGER UNIQUE,
            abgeordneter_id INTEGER REFERENCES abgeordnete(id),
            organisation TEXT NOT NULL,
            taetigkeit TEXT,
            einkommensklasse TEXT,   -- z.B. "Stufe 3: 15.001–30.000 €"
            betrag_min REAL,         -- untere Grenze der Stufe
            betrag_max REAL,         -- obere Grenze der Stufe
            branche TEXT,
            beginn TEXT,
            ende TEXT,
            aktiv INTEGER DEFAULT 1
        );

        -- NEU: Ausschussmitgliedschaften
        CREATE TABLE IF NOT EXISTS ausschuesse (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ausschuss_id TEXT,
            ausschuss_name TEXT NOT NULL,
            abgeordneter_id INTEGER REFERENCES abgeordnete(id),
            rolle TEXT,              -- z.B. "Mitglied", "Vorsitz", "Stellv."
            partei TEXT
        );

        -- NEU: Drehtür-Effekte
        CREATE TABLE IF NOT EXISTS drehtuer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            abgeordneter_id INTEGER REFERENCES abgeordnete(id),
            name TEXT,
            partei TEXT,
            position_vorher TEXT,    -- Amt / Ausschuss im Bundestag
            organisation_nachher TEXT, -- Unternehmen / Verband danach
            branche TEXT,
            wechsel_datum TEXT,
            verdachts_score REAL,
            begruendung TEXT
        );

        -- NEU: Super-Score (kombiniert alle Faktoren)
        CREATE TABLE IF NOT EXISTS super_korrelationen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            abgeordneter_id INTEGER REFERENCES abgeordnete(id),
            spende_id INTEGER REFERENCES parteispenden(id),
            abstimmung_id INTEGER REFERENCES abstimmungen(id),
            nebeneinkuenfte_id INTEGER REFERENCES nebeneinkuenfte(id),
            ausschuss_id INTEGER REFERENCES ausschuesse(id),
            super_score REAL,
            faktoren TEXT,           -- JSON: welche Faktoren zum Score beitragen
            erstellt_am TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    print("✓ Datenbank v2 bereit")


# ═══════════════════════════════════════════════════════════
# BRANCHENERKENNUNG
# ═══════════════════════════════════════════════════════════

BRANCHE_KEYWORDS = {
    "Finanzsektor":       ["bank","finanz","invest","capital","allianz","axa","generali","commerzbank","sparkasse","volksbank","versicherung","fonds","asset"],
    "Automobilindustrie": ["bmw","volkswagen","vw ","daimler","mercedes","porsche","audi","opel","ford","bosch","continental","schaeffler","auto","fahrzeug","kfz"],
    "Energiesektor":      ["rwe","e.on","eon","vattenfall","uniper","engie","energie","energy","solar","wind","strom","gas","getec","erdgas","kraftwerk"],
    "Rüstung":            ["rheinmetall","krauss","heckler","thyssen","airbus","diehl","hensoldt","defence","defense","rüstung","waffen","militär"],
    "Pharma":             ["bayer","novartis","roche","pfizer","sanofi","merck","boehringer","pharma","medizin","arzneimittel","biotech"],
    "Telekommunikation":  ["telekom","telefonica","vodafone","1&1","freenet","sap","ibm","microsoft","software","digital","tech","it ","internet"],
    "Chemie":             ["basf","chemie","chemical","covestro","lanxess","evonik","henkel","wacker","dow ","dupont"],
    "Immobilien":         ["immobilien","real estate","wohn","haus","bau","hochtief","strabag","bilfinger","vonovia","deutsche wohnen"],
    "Luftfahrt":          ["lufthansa","air berlin","condor","flug","aviation","fraport","airport","flughafen"],
    "Medien":             ["springer","bertelsmann","burda","funke","verlag","medien","media","zeitung","rundfunk"],
    "Lobbyverband":       ["verband","bundesverband","zentralverband","industrieverband","arbeitgeber","gewerkschaft","bdi","vda","vdma","bvr"],
    "Beratung":           ["mckinsey","bcg","roland berger","kpmg","deloitte","pwc","ernst","consulting","beratung","anwalt","rechtsanwalt","kanzlei"],
}

def erkenne_branche(text):
    t = (text or "").lower()
    for branche, kws in BRANCHE_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return branche
    return "Sonstige"


# ═══════════════════════════════════════════════════════════
# QUELLE 1: PARTEISPENDEN (echte Daten aus öffentlichen Quellen)
# ═══════════════════════════════════════════════════════════

# Bekannte Großspenden aus Bundestagsdrucksachen und Medienberichten
# Quellen: bundestag.de, abgeordnetenwatch.de, lobbycontrol.de
BEKANNTE_SPENDEN = [
    # 2021
    ("2021-09-01", "Steven Schuurman", "Grüne", 1250000, "Tech/IT"),
    ("2021-09-01", "Moritz Schmidt", "Grüne", 1000300, "Tech/IT"),
    ("2021-08-01", "Georg Kofler", "FDP", 750000, "Medien"),
    ("2021-08-15", "Christoph Kahl", "FDP", 750000, "Immobilien"),
    ("2021-06-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 530000, "Finanzwesen"),
    ("2021-08-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 265000, "Finanzwesen"),
    ("2021-07-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CDU", 250000, "Industrie"),
    ("2021-08-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CSU", 250000, "Industrie"),
    ("2021-09-01", "BMW AG", "CDU", 100000, "Automobil"),
    ("2021-09-01", "BMW AG", "FDP", 100000, "Automobil"),
    ("2021-09-01", "BMW AG", "CSU", 100000, "Automobil"),
    ("2021-09-01", "Daimler AG", "CDU", 100000, "Automobil"),
    ("2021-09-01", "Daimler AG", "FDP", 100000, "Automobil"),
    ("2021-09-01", "Allianz SE", "CDU", 100000, "Finanzwesen"),
    ("2021-06-01", "Sixt GmbH & Co. Autovermietung KG", "CDU", 250000, "Automobil"),
    ("2021-06-01", "Sixt GmbH & Co. Autovermietung KG", "FDP", 250000, "Automobil"),
    # 2022
    ("2022-05-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 265000, "Finanzwesen"),
    ("2022-06-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CDU", 200000, "Industrie"),
    ("2022-06-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CSU", 200000, "Industrie"),
    ("2022-05-01", "Klaus-Michael Kühne", "CDU", 250000, "Logistik"),
    ("2022-05-01", "Klaus-Michael Kühne", "FDP", 150000, "Logistik"),
    # 2023
    ("2023-05-01", "Klaus-Michael Kühne", "CDU", 65000, "Logistik"),
    ("2023-06-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CDU", 569962, "Industrie"),
    ("2023-07-01", "Südschleswig-Ausschuss", "SSW", 250000, "Sonstige"),
    ("2023-08-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 265000, "Finanzwesen"),
    ("2023-09-01", "Sixt GmbH & Co. Autovermietung KG", "FDP", 100000, "Automobil"),
    ("2023-10-01", "Sixt GmbH & Co. Autovermietung KG", "CSU", 100000, "Automobil"),
    # 2024
    ("2024-01-08", "Thomas Stanger", "BSW", 990000, "Sonstige"),
    ("2024-03-13", "Thomas Stanger", "BSW", 4090000, "Sonstige"),
    ("2024-10-01", "BSW - Fuer Vernunft und Gerechtigkeit e.V.", "BSW", 1200000, "Sonstige"),
    ("2024-01-01", "Joh. Berenberg Gossler & Co. KG", "CDU", 322500, "Finanzwesen"),
    ("2024-06-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 530000, "Finanzwesen"),
    ("2024-05-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CDU", 300000, "Industrie"),
    ("2024-05-01", "Verband der Bayerischen Metall- und Elektro-Industrie", "CSU", 250000, "Industrie"),
    ("2024-08-01", "Sixt GmbH & Co. Autovermietung KG", "CDU", 100000, "Automobil"),
    ("2024-08-01", "Sixt GmbH & Co. Autovermietung KG", "CSU", 100000, "Automobil"),
    ("2024-08-01", "Sixt GmbH & Co. Autovermietung KG", "SPD", 90000, "Automobil"),
    ("2024-03-01", "Dr. Theiss Naturwaren GmbH", "CDU", 126500, "Gesundheit"),
    ("2024-06-01", "Dr. Theiss Naturwaren GmbH", "SPD", 65000, "Gesundheit"),
    ("2024-11-01", "Campact e.V.", "SPD", 200000, "Lobbyverband"),
    ("2024-11-01", "Campact e.V.", "Grüne", 200000, "Lobbyverband"),
    ("2024-11-01", "Campact e.V.", "Linke", 100000, "Lobbyverband"),
    ("2024-11-01", "Harald Christ", "CDU", 40000, "Finanzwesen"),
    ("2024-11-01", "Harald Christ", "SPD", 40000, "Finanzwesen"),
    ("2024-11-01", "Harald Christ", "FDP", 40000, "Finanzwesen"),
    ("2024-11-01", "Harald Christ", "Grüne", 40000, "Finanzwesen"),
    # 2025
    ("2025-01-01", "DVAG Deutsche Vermögensberatung AG", "CDU", 530000, "Finanzwesen"),
    ("2025-01-15", "Verband der Bayerischen Metall- und Elektro-Industrie", "CDU", 250000, "Industrie"),
    ("2025-01-15", "Verband der Bayerischen Metall- und Elektro-Industrie", "CSU", 200000, "Industrie"),
    ("2025-01-20", "Sixt GmbH & Co. Autovermietung KG", "CDU", 150000, "Automobil"),
    ("2025-01-20", "Sixt GmbH & Co. Autovermietung KG", "CSU", 100000, "Automobil"),
    ("2025-02-01", "Klaus-Michael Kühne", "CDU", 500000, "Logistik"),
]

def lade_parteispenden(jahre=None):
    """Lädt echte Parteispenden aus öffentlichen Quellen."""
    if jahre is None:
        jahre = list(range(2021, datetime.date.today().year + 1))
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    gesamt = 0

    # 1. Hartcodierte bekannte Spenden laden
    for eintrag in BEKANNTE_SPENDEN:
        datum, spender, empfaenger, betrag, branche = eintrag
        if int(datum[:4]) not in jahre:
            continue
        if not cur.execute("SELECT id FROM parteispenden WHERE spender=? AND datum=? AND betrag_eur=?",
                           (spender, datum, betrag)).fetchone():
            cur.execute("INSERT INTO parteispenden (spender,empfaenger,betrag_eur,datum,branche,quelle_url,quelle) VALUES (?,?,?,?,?,?,?)",
                        (spender, empfaenger, betrag, datum, branche,
                         "https://www.bundestag.de/parteienfinanzierung", "bundestag"))
            gesamt += 1

    # 2. Versuche Wikipedia für neuere Einträge
    try:
        wp_url = "https://de.wikipedia.org/w/api.php?action=parse&page=Parteispende&prop=wikitext&format=json"
        data = api_get(wp_url, cache_key="wikipedia_parteispende", cache_h=24)
        wikitext = ""
        if data and "parse" in data:
            wikitext = data["parse"].get("wikitext", {}).get("*", "")

        if wikitext:
            monat_map = {'Januar':'01','Februar':'02','März':'03','April':'04',
                         'Mai':'05','Juni':'06','Juli':'07','August':'08',
                         'September':'09','Oktober':'10','November':'11','Dezember':'12'}
            zeilen = wikitext.split('\n')
            current_jahr = None
            for zeile in zeilen:
                jahr_match = re.search(r'==\s*(\d{4})\s*==', zeile)
                if jahr_match:
                    current_jahr = int(jahr_match.group(1))
                    continue
                if current_jahr not in jahre:
                    continue
                if not zeile.startswith('|') or zeile.startswith('|-') or zeile.startswith('|+'):
                    continue
                parts = [p.strip() for p in zeile.strip('|').split('||')]
                if len(parts) < 4:
                    parts = [p.strip() for p in re.split(r'\|(?!\|)', zeile.strip('|'))]
                if len(parts) < 4:
                    continue
                try:
                    datum_raw = re.sub(r'\[\[.*?\]\]', '', parts[0])
                    datum_raw = re.sub(r'<[^>]+>', '', datum_raw).strip()
                    dm = re.search(r'(\d{1,2})\.\s*(\w+)(?:\s+(\d{4}))?', datum_raw)
                    if dm:
                        tag = dm.group(1).zfill(2)
                        monat = monat_map.get(dm.group(2), '01')
                        jahr_d = dm.group(3) or str(current_jahr)
                        datum = f"{jahr_d}-{monat}-{tag}"
                    else:
                        datum = f"{current_jahr}-01-01"
                    spender = re.sub(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]', r'\1', parts[1])
                    spender = re.sub(r'<[^>]+>', '', spender).strip()[:200]
                    empfaenger = re.sub(r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]', r'\1', parts[2])
                    empfaenger = re.sub(r'<[^>]+>', '', empfaenger).strip()[:100]
                    betrag_raw = re.sub(r'<[^>]+>|\[\[.*?\]\]', '', parts[3])
                    betrag_raw = betrag_raw.replace('.','').replace(',','.').replace('€','').replace('Euro','').replace('\xa0','').strip()
                    betrag_raw = ''.join(ch for ch in betrag_raw if ch.isdigit() or ch == '.')
                    if not betrag_raw:
                        continue
                    betrag = float(betrag_raw)
                    if betrag < 35000 or not spender or not empfaenger:
                        continue
                    if not cur.execute("SELECT id FROM parteispenden WHERE spender=? AND datum=? AND betrag_eur=?",
                                       (spender, datum, betrag)).fetchone():
                        cur.execute("INSERT INTO parteispenden (spender,empfaenger,betrag_eur,datum,branche,quelle_url,quelle) VALUES (?,?,?,?,?,?,?)",
                                    (spender, empfaenger, betrag, datum,
                                     erkenne_branche(spender),
                                     "https://de.wikipedia.org/wiki/Parteispende", "wikipedia"))
                        gesamt += 1
                except (ValueError, IndexError, AttributeError):
                    continue
    except Exception as e:
        print(f"  ⚠ Wikipedia: {e}")

    print(f"  ✓ {gesamt} Spenden geladen")
    conn.commit()
    conn.close()
    print(f"  ✓ Spenden gesamt neu: {gesamt}")
    return gesamt


# LEGACY (nicht mehr verwendet)
def _parse_spenden_drucksache(text, drucksache_url, c):
    """Parst Text einer Parteispenden-Drucksache und speichert Einträge."""
    neu = 0
    # Suche nach Zeilen mit Betrag, Spender, Partei
    # Format: Datum | Spender | Partei | Betrag
    zeilen = text.split('\n')
    for zeile in zeilen:
        zeile = zeile.strip()
        # Suche nach Datumsmustern DD.MM.YYYY
        datum_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', zeile)
        betrag_match = re.search(r'([\d\.]+(?:,\d+)?)\s*(?:Euro|EUR|€)', zeile, re.IGNORECASE)
        if not datum_match or not betrag_match:
            continue
        try:
            t = datum_match.group(1).split('.')
            datum = f"{t[2]}-{t[1].zfill(2)}-{t[0].zfill(2)}"
            bs = betrag_match.group(1).replace('.','').replace(',','.')
            betrag = float(bs)
            if betrag < 35000:
                continue
            # Parteinamen suchen
            partei = "Unbekannt"
            for p in ["CDU","CSU","SPD","Grüne","FDP","AfD","Linke","BSW","SSW","Volt","Freie Wähler"]:
                if p.lower() in zeile.lower():
                    partei = p
                    break
            # Spender: Rest der Zeile nach Datum
            spender_part = zeile[datum_match.end():betrag_match.start()].strip(' -|,')
            spender = spender_part[:100] if spender_part else "Unbekannt"
            if spender == "Unbekannt" or betrag <= 0:
                continue
            if not c.execute("SELECT id FROM parteispenden WHERE spender=? AND datum=? AND betrag_eur=?",
                             (spender, datum, betrag)).fetchone():
                c.execute("INSERT INTO parteispenden (spender,empfaenger,betrag_eur,datum,branche,quelle_url,quelle) VALUES (?,?,?,?,?,?,?)",
                          (spender, partei, betrag, datum, erkenne_branche(spender), drucksache_url, "dip"))
                neu += 1
        except (ValueError, IndexError):
            continue
    return neu




# ═══════════════════════════════════════════════════════════
# QUELLE 2: ABGEORDNETE
# ═══════════════════════════════════════════════════════════

def lade_abgeordnete():
    print(f"  → Abgeordnete (Periode {WAHLPERIODE_ID})...")
    url = f"https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates?parliament_period={WAHLPERIODE_ID}&pager_limit=500"
    data = api_get(url, cache_key=f"abgeordnete_{WAHLPERIODE_ID}")
    if not data or "data" not in data:
        return 0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    n = 0
    for m in data["data"]:
        try:
            pol = m.get("politician") or {}
            party = pol.get("party") or {}
            if not pol.get("id") or not pol.get("label"):
                continue
            c.execute("""INSERT INTO abgeordnete (aw_id,name,partei,aktiv) VALUES (?,?,?,1)
                ON CONFLICT(aw_id) DO UPDATE SET name=excluded.name, partei=excluded.partei""",
                (pol["id"], pol["label"], party.get("label","?")))
            n += 1
        except Exception:
            continue
    conn.commit()
    conn.close()
    print(f"  ✓ {n} Abgeordnete")
    return n


# ═══════════════════════════════════════════════════════════
# QUELLE 3 & 4: ABSTIMMUNGEN + ERGEBNISSE
# ═══════════════════════════════════════════════════════════

def ermittle_thema(titel):
    t = titel.lower()
    if any(w in t for w in ["rüstung","waffe","bundeswehr","verteidigung","nato","ukraine","militär"]):
        return "Verteidigung"
    if any(w in t for w in ["energie","solar","wind","atom","kohle","strom","gas","klima","co2"]):
        return "Energie"
    if any(w in t for w in ["steuer","haushalt","schulden","finanz","budget","abgabe"]):
        return "Finanzen"
    if any(w in t for w in ["gesundheit","pflege","kranken","pharma","impf","medizin","rente","sozial"]):
        return "Gesundheit"
    if any(w in t for w in ["digital","internet","daten","cyber","tech","software","ki ","künstliche"]):
        return "Telekommunikation"
    if any(w in t for w in ["straße","bahn","auto","infrastruktur","verkehr","mobilität","autobahn"]):
        return "Infrastruktur"
    if any(w in t for w in ["wirtschaft","unternehmen","handel","export","markt","industrie","lieferkette"]):
        return "Wirtschaft"
    return "Sonstiges"


def lade_abstimmungen(max_seiten=10):
    print(f"  → Abstimmungen ({max_seiten} Seiten)...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    n = 0
    for seite in range(max_seiten):
        data = api_get(
            f"https://www.abgeordnetenwatch.de/api/v2/polls?parliament_period_id={WAHLPERIODE_ID}&pager_limit=25&page={seite}",
            cache_key=f"polls_{WAHLPERIODE_ID}_p{seite}"
        )
        if not data or not data.get("data"):
            break
        for poll in data["data"]:
            try:
                if not poll.get("id") or not poll.get("label") or not poll.get("field_poll_date"):
                    continue
                c.execute("""INSERT INTO abstimmungen (aw_id,titel,datum,thema) VALUES (?,?,?,?)
                    ON CONFLICT(aw_id) DO UPDATE SET titel=excluded.titel, datum=excluded.datum""",
                    (poll["id"], poll["label"], poll["field_poll_date"], ermittle_thema(poll["label"])))
                n += 1
            except Exception:
                continue
    conn.commit()
    conn.close()
    print(f"  ✓ {n} Abstimmungen")
    return n


def lade_abstimmungsergebnisse(max_abstimmungen=40):
    print(f"  → Abstimmungsergebnisse (max. {max_abstimmungen})...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    offen = c.execute("""
        SELECT a.id, a.aw_id FROM abstimmungen a
        WHERE NOT EXISTS (SELECT 1 FROM abstimmungs_ergebnis e WHERE e.abstimmung_id=a.id)
        ORDER BY a.datum DESC LIMIT ?
    """, (max_abstimmungen,)).fetchall()
    n = 0
    vmap = {"yes":"ja","no":"nein","abstain":"enthalten","no_show":"abwesend"}
    for abst_id, aw_id in offen:
        data = api_get(f"https://www.abgeordnetenwatch.de/api/v2/polls/{aw_id}?related_data=votes",
                       cache_key=f"votes_{aw_id}")
        if not data or "data" not in data:
            continue
        d = data["data"]
        if isinstance(d, list): continue
        related = (d.get("related_data") or {})
        votes_raw = related.get("votes", {})
        votes = votes_raw if isinstance(votes_raw, list) else votes_raw.get("data", [])
        for vote in votes:
            try:
                pol   = (vote.get("mandate") or {}).get("politician") or {}
                party = (pol.get("party") or {})
                row   = c.execute("SELECT id FROM abgeordnete WHERE aw_id=?", (pol.get("id"),)).fetchone()
                if not row:
                    continue
                c.execute("INSERT OR IGNORE INTO abstimmungs_ergebnis (abstimmung_id,abgeordneter_id,votum,partei) VALUES (?,?,?,?)",
                          (abst_id, row[0], vmap.get(vote.get("vote",""), vote.get("vote","")), party.get("label","?")))
                n += 1
            except Exception:
                continue
    conn.commit()
    conn.close()
    print(f"  ✓ {n} Stimmzettel")
    return n


# ═══════════════════════════════════════════════════════════
# QUELLE 5 (NEU): NEBENEINKÜNFTE
# ═══════════════════════════════════════════════════════════

# Einkommensklassen → Zahlenwerte
EINKOMMENSKLASSEN = {
    "1": (1000, 3500),
    "2": (3500, 7000),
    "3": (7000, 15000),
    "4": (15000, 30000),
    "5": (30000, 50000),
    "6": (50000, 75000),
    "7": (75000, 100000),
    "8": (100000, 150000),
    "9": (150000, 250000),
    "10": (250000, 999999),
}

def lade_nebeneinkuenfte(max_seiten=20):
    """
    Lädt alle Nebeneinkünfte der Bundestagsabgeordneten.
    API: abgeordnetenwatch.de/api/v2/sidejobs
    Enthält: Organisation, Tätigkeit, Einkommensklasse (Stufe 1-10)
    """
    print(f"  → Nebeneinkünfte ({max_seiten} Seiten)...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    n = 0

    for seite in range(max_seiten):
        data = api_get(
            f"https://www.abgeordnetenwatch.de/api/v2/sidejobs?parliament_period={WAHLPERIODE_ID}&pager_limit=50&page={seite}",
            cache_key=f"sidejobs_{WAHLPERIODE_ID}_p{seite}"
        )
        if not data or not data.get("data"):
            break

        for job in data["data"]:
            try:
                aw_sid   = job.get("id")
                mandate  = job.get("mandate") or {}
                pol      = (mandate.get("politician") or mandate) or {}
                org      = job.get("sidejob_organization") or {}
                taet     = job.get("label", "")
                einkomm  = str(job.get("income_level") or "")
                beginn   = job.get("valid_from") or ""
                ende     = job.get("valid_until") or ""
                org_name = org.get("label", "")

                aw_pol_id = pol.get("id") or mandate.get("politician_id")
                row = c.execute("SELECT id FROM abgeordnete WHERE aw_id=?", (aw_pol_id,)).fetchone()
                if not row or not org_name:
                    continue
                abg_db_id = row[0]

                bmin, bmax = EINKOMMENSKLASSEN.get(einkomm, (0, 0))
                branche = erkenne_branche(org_name + " " + taet)

                c.execute("""
                    INSERT INTO nebeneinkuenfte
                    (aw_id,abgeordneter_id,organisation,taetigkeit,einkommensklasse,betrag_min,betrag_max,branche,beginn,ende,aktiv)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1)
                    ON CONFLICT(aw_id) DO UPDATE SET
                        organisation=excluded.organisation, taetigkeit=excluded.taetigkeit,
                        betrag_min=excluded.betrag_min, betrag_max=excluded.betrag_max,
                        branche=excluded.branche
                """, (aw_sid, abg_db_id, org_name, taet, f"Stufe {einkomm}", bmin, bmax, branche, beginn, ende))
                n += 1
            except Exception:
                continue

    conn.commit()
    conn.close()
    print(f"  ✓ {n} Nebeneinkünfte geladen")
    return n


# ═══════════════════════════════════════════════════════════
# QUELLE 6 (NEU): AUSSCHÜSSE
# ═══════════════════════════════════════════════════════════

AUSSCHUSS_THEMA_MAP = {
    "Verteidigung":           "Verteidigung",
    "Haushalt":               "Finanzen",
    "Finanzen":               "Finanzen",
    "Wirtschaft":             "Wirtschaft",
    "Energie":                "Energie",
    "Klimaschutz":            "Energie",
    "Umwelt":                 "Energie",
    "Gesundheit":             "Gesundheit",
    "Digitale Agenda":        "Telekommunikation",
    "Verkehr":                "Infrastruktur",
    "Bau":                    "Immobilien",
    "Außenpolitik":           "Verteidigung",
    "Recht":                  "Sonstiges",
    "Innenpolitik":           "Sonstiges",
}

def lade_ausschuesse():
    """
    Lädt alle Ausschüsse und ihre Mitglieder.
    API: bundestag.de/xml/v2/ausschuesse/index.xml
    """
    print("  → Ausschüsse (Bundestag XML API)...")
    xml = xml_get("https://www.bundestag.de/xml/v2/ausschuesse/index.xml", cache_key="ausschuesse_index")
    if not xml:
        print("  ⚠ Ausschuss-Index nicht erreichbar")
        return 0

    # Alle Ausschuss-IDs extrahieren
    ausschuss_ids = re.findall(r'<item id="([^"]+)"', xml)
    if not ausschuss_ids:
        # Alternativer Pattern
        ausschuss_ids = re.findall(r'id="([A-Z0-9]+)"', xml)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Alte Einträge löschen
    c.execute("DELETE FROM ausschuesse")
    n = 0

    for aid in ausschuss_ids[:30]:  # max 30 Ausschüsse
        xml_detail = xml_get(
            f"https://www.bundestag.de/xml/v2/ausschuesse/{aid}.xml",
            cache_key=f"ausschuss_{aid}"
        )
        if not xml_detail:
            continue

        # Ausschussname extrahieren
        namen = re.findall(r'<name[^>]*>([^<]+)</name>', xml_detail)
        ausschuss_name = namen[0].strip() if namen else f"Ausschuss {aid}"

        # Thema ermitteln
        thema = "Sonstiges"
        for key, wert in AUSSCHUSS_THEMA_MAP.items():
            if key.lower() in ausschuss_name.lower():
                thema = wert
                break

        # Mitglieder extrahieren
        mitglieder = re.findall(
            r'<mitglied[^>]*>(.*?)</mitglied>', xml_detail, re.DOTALL
        )
        for m in mitglieder:
            name_match  = re.search(r'<name>([^<]+)</name>', m)
            rolle_match = re.search(r'<funktion>([^<]+)</funktion>', m)
            partei_match = re.search(r'<fraktion>([^<]+)</fraktion>', m)
            if not name_match:
                continue
            name   = name_match.group(1).strip()
            rolle  = rolle_match.group(1).strip() if rolle_match else "Mitglied"
            partei = partei_match.group(1).strip() if partei_match else ""

            # Abgeordneten in DB finden
            row = c.execute(
                "SELECT id FROM abgeordnete WHERE name LIKE ?",
                (f"%{name.split(',')[0].strip()}%",)
            ).fetchone()
            abg_id = row[0] if row else None

            c.execute("""
                INSERT INTO ausschuesse (ausschuss_id, ausschuss_name, abgeordneter_id, rolle, partei)
                VALUES (?,?,?,?,?)
            """, (aid, ausschuss_name, abg_id, rolle, partei))
            n += 1

    conn.commit()
    conn.close()
    print(f"  ✓ {n} Ausschussmitgliedschaften geladen")
    return n


# ═══════════════════════════════════════════════════════════
# QUELLE 7 (NEU): DREHTÜR-EFFEKT
# ═══════════════════════════════════════════════════════════

def berechne_drehtuer():
    """
    Erkennt den Drehtür-Effekt: Abgeordnete die nach dem Mandat
    in Branchen wechseln, in denen sie Nebeneinkünfte hatten.
    Auch: Abgeordnete mit aktiven Nebeneinkünften in relevanten Ausschüssen.
    """
    print("  → Drehtür-Analyse...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM drehtuer")
    n = 0

    # Abgeordnete mit Nebeneinkünften + Ausschussmitgliedschaft in gleicher Branche
    verdaechtige = c.execute("""
        SELECT DISTINCT
            a.id, a.name, a.partei,
            n.organisation, n.branche, n.betrag_min, n.betrag_max,
            au.ausschuss_name, au.rolle
        FROM abgeordnete a
        JOIN nebeneinkuenfte n ON n.abgeordneter_id = a.id
        JOIN ausschuesse au   ON au.abgeordneter_id = a.id
        WHERE n.betrag_min > 0
    """).fetchall()

    AUSSCHUSS_BRANCHE_MAP = {
        "Verteidigung": ["Rüstung"],
        "Finanzen":     ["Finanzsektor","Versicherung","Beratung"],
        "Wirtschaft":   ["Automobilindustrie","Chemie","Beratung","Lobbyverband"],
        "Energie":      ["Energiesektor","Chemie"],
        "Gesundheit":   ["Pharma","Versicherung"],
        "Telekommunikation": ["Telekommunikation"],
        "Infrastruktur":["Automobilindustrie","Immobilien","Luftfahrt"],
    }

    for abg_id, name, partei, org, branche, bmin, bmax, ausschuss, rolle in verdaechtige:
        ausschuss_thema = "Sonstiges"
        for key, wert in AUSSCHUSS_THEMA_MAP.items():
            if key.lower() in ausschuss.lower():
                ausschuss_thema = wert
                break

        relevante_branchen = AUSSCHUSS_BRANCHE_MAP.get(ausschuss_thema, [])
        if branche not in relevante_branchen:
            continue

        # Score berechnen
        score = 0.5  # Basis: Ausschuss + Nebeneinkünfte in gleicher Branche
        if bmin >= 50000:   score += 0.25
        elif bmin >= 15000: score += 0.15
        elif bmin >= 7000:  score += 0.10
        if "Vorsitz" in rolle: score += 0.15
        elif "Stellv" in rolle: score += 0.10
        score = round(min(score, 1.0), 2)

        gruende = [
            f"Ausschuss '{ausschuss}' ({rolle})",
            f"Nebeneinkünfte bei '{org}' (Branche: {branche})",
        ]
        if bmin > 0:
            gruende.append(f"Einkommensstufe: {bmin:,.0f}–{bmax:,.0f}€/Jahr")

        c.execute("""
            INSERT INTO drehtuer
            (abgeordneter_id,name,partei,position_vorher,organisation_nachher,branche,verdachts_score,begruendung)
            VALUES (?,?,?,?,?,?,?,?)
        """, (abg_id, name, partei, f"{ausschuss} ({rolle})", org, branche, score, " | ".join(gruende)))
        n += 1

    conn.commit()
    conn.close()
    print(f"  ✓ {n} Drehtür-Verdachtsfälle")
    return n


# ═══════════════════════════════════════════════════════════
# SUPER-KORRELATION (kombiniert alle Faktoren)
# ═══════════════════════════════════════════════════════════

BRANCHE_THEMA_MAP = {
    "Finanzsektor":       ["Finanzen","Wirtschaft"],
    "Automobilindustrie": ["Infrastruktur","Wirtschaft"],
    "Energiesektor":      ["Energie"],
    "Versicherung":       ["Finanzen","Gesundheit"],
    "Rüstung":            ["Verteidigung"],
    "Pharma":             ["Gesundheit"],
    "Telekommunikation":  ["Telekommunikation"],
    "Luftfahrt":          ["Infrastruktur","Finanzen"],
    "Chemie":             ["Wirtschaft","Energie"],
    "Immobilien":         ["Wirtschaft","Infrastruktur"],
    "Lobbyverband":       ["Wirtschaft","Finanzen"],
    "Beratung":           ["Wirtschaft","Finanzen"],
}

def analysiere_korrelationen():
    """Berechnet Standard-Korrelationen (Spende → Abstimmung)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM korrelationen")
    spenden      = c.execute("SELECT id,spender,empfaenger,betrag_eur,datum,branche FROM parteispenden").fetchall()
    abstimmungen = c.execute("SELECT id,titel,datum,thema FROM abstimmungen WHERE datum!=''").fetchall()
    if not spenden or not abstimmungen:
        conn.close()
        return 0

    n = 0
    for sp_id, spender, partei, betrag, sp_datum, branche in spenden:
        try:
            sp_date = datetime.date.fromisoformat(sp_datum[:10])
        except ValueError:
            continue
        themen = BRANCHE_THEMA_MAP.get(branche or "", [])

        for abst_id, titel, abst_datum, thema in abstimmungen:
            try:
                delta = (datetime.date.fromisoformat(abst_datum[:10]) - sp_date).days
            except ValueError:
                continue
            if not (0 < delta <= 180):
                continue

            res = c.execute("""
                SELECT SUM(CASE WHEN votum='ja' THEN 1 ELSE 0 END), COUNT(*)
                FROM abstimmungs_ergebnis WHERE abstimmung_id=? AND partei=?
            """, (abst_id, partei)).fetchone()
            partei_ja     = res and res[1] > 0 and (res[0] / res[1]) > 0.5
            branche_match = thema in themen

            score = 0.0
            if betrag >= 400000:   score += 0.30
            elif betrag >= 200000: score += 0.20
            elif betrag >= 100000: score += 0.10
            elif betrag >= 35000:  score += 0.05
            if delta <= 30:   score += 0.30
            elif delta <= 60: score += 0.20
            elif delta <= 90: score += 0.10
            if branche_match: score += 0.25
            if partei_ja:     score += 0.15
            score = round(min(score, 1.0), 2)

            if score >= 0.25:
                gruende = []
                if betrag >= 35000:  gruende.append(f"Spende {betrag:,.0f}€")
                if delta <= 60:      gruende.append(f"Zeitnah ({delta} Tage)")
                if branche_match:    gruende.append(f"'{branche}' → '{thema}'")
                if partei_ja:        gruende.append(f"{partei} stimmte Ja")
                c.execute("INSERT INTO korrelationen (spende_id,abstimmung_id,tage_abstand,verdachts_score,begruendung) VALUES (?,?,?,?,?)",
                          (sp_id, abst_id, delta, score, " | ".join(gruende)))
                n += 1

    conn.commit()
    conn.close()
    print(f"  ✓ {n} Standard-Korrelationen")
    return n


def berechne_super_scores():
    """
    Super-Score: kombiniert Spende + Nebeneinkünfte + Ausschuss + Abstimmung.
    Das ist der mächtigste Indikator im System.
    """
    print("  → Super-Score Berechnung...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM super_korrelationen")
    n = 0

    # Finde Abgeordnete die:
    # 1. Nebeneinkünfte in einer Branche haben
    # 2. Im zuständigen Ausschuss sitzen
    # 3. Deren Partei eine Spende aus der gleichen Branche erhielt
    # 4. Für die begünstigende Abstimmung gestimmt haben
    kandidaten = c.execute("""
        SELECT DISTINCT
            abg.id, abg.name, abg.partei,
            ne.id as ne_id, ne.organisation, ne.branche as ne_branche,
            ne.betrag_min, ne.betrag_max,
            au.id as au_id, au.ausschuss_name, au.rolle
        FROM abgeordnete abg
        JOIN nebeneinkuenfte ne ON ne.abgeordneter_id = abg.id AND ne.betrag_min >= 1000
        JOIN ausschuesse au     ON au.abgeordneter_id = abg.id
        WHERE abg.partei IS NOT NULL
    """).fetchall()

    for abg_id, abg_name, partei, ne_id, ne_org, ne_branche, bmin, bmax, au_id, ausschuss, rolle in kandidaten:
        rel_themen = BRANCHE_THEMA_MAP.get(ne_branche or "", [])
        if not rel_themen:
            continue

        # Spenden an die Partei aus gleicher Branche
        spenden = c.execute("""
            SELECT id, spender, betrag_eur, datum FROM parteispenden
            WHERE empfaenger=? AND branche=?
        """, (partei, ne_branche)).fetchall()

        for sp_id, spender, betrag, sp_datum in spenden:
            try:
                sp_date = datetime.date.fromisoformat(sp_datum[:10])
            except ValueError:
                continue

            # Passende Abstimmungen
            abstimmungen = c.execute("""
                SELECT a.id, a.titel, a.datum, a.thema
                FROM abstimmungen a
                WHERE a.thema IN ({})
            """.format(','.join('?'*len(rel_themen))), rel_themen).fetchall()

            for abst_id, titel, abst_datum, thema in abstimmungen:
                try:
                    delta = (datetime.date.fromisoformat(abst_datum[:10]) - sp_date).days
                except ValueError:
                    continue
                if not (0 < delta <= 180):
                    continue

                # Hat der Abgeordnete Ja gestimmt?
                votum_row = c.execute("""
                    SELECT votum FROM abstimmungs_ergebnis
                    WHERE abstimmung_id=? AND abgeordneter_id=?
                """, (abst_id, abg_id)).fetchone()
                hat_ja_gestimmt = votum_row and votum_row[0] == "ja"

                # Super-Score zusammensetzen
                faktoren = {}
                score = 0.0

                # Faktor 1: Nebeneinkünfte (max 0.25)
                if bmin >= 50000:   s = 0.25
                elif bmin >= 15000: s = 0.20
                elif bmin >= 7000:  s = 0.15
                elif bmin >= 3500:  s = 0.10
                else:               s = 0.05
                score += s
                faktoren["nebeneinkuenfte"] = s

                # Faktor 2: Ausschuss (max 0.25)
                ausschuss_thema = ermittle_thema(ausschuss)
                if ausschuss_thema in rel_themen:
                    s = 0.25 if "Vorsitz" in rolle else (0.20 if "Stellv" in rolle else 0.15)
                    score += s
                    faktoren["ausschuss"] = s

                # Faktor 3: Spende (max 0.20)
                if betrag >= 400000:   s = 0.20
                elif betrag >= 200000: s = 0.15
                elif betrag >= 100000: s = 0.10
                elif betrag >= 35000:  s = 0.05
                else:                  s = 0.0
                score += s
                faktoren["spende"] = s

                # Faktor 4: Zeitnähe (max 0.15)
                if delta <= 30:   s = 0.15
                elif delta <= 60: s = 0.10
                elif delta <= 90: s = 0.05
                else:             s = 0.0
                score += s
                faktoren["zeitnaehe"] = s

                # Faktor 5: Ja-Votum (max 0.15)
                if hat_ja_gestimmt:
                    score += 0.15
                    faktoren["ja_votum"] = 0.15

                score = round(min(score, 1.0), 2)
                if score >= 0.4:
                    c.execute("""
                        INSERT INTO super_korrelationen
                        (abgeordneter_id,spende_id,abstimmung_id,nebeneinkuenfte_id,ausschuss_id,super_score,faktoren)
                        VALUES (?,?,?,?,?,?,?)
                    """, (abg_id, sp_id, abst_id, ne_id, au_id, score, json.dumps(faktoren)))
                    n += 1

    conn.commit()
    conn.close()
    print(f"  ✓ {n} Super-Score Einträge")
    return n


# ═══════════════════════════════════════════════════════════
# DEMO-DATEN
# ═══════════════════════════════════════════════════════════

def lade_demo_daten():
    print("  → Demo-Daten...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Abgeordnete
    abgeordnete = [
        (1,1,"Friedrich Merz","CDU","Hochsauerlandkreis"),
        (2,2,"Olaf Scholz","SPD","Hamburg-Altona"),
        (3,3,"Robert Habeck","Grüne","Flensburg-Schleswig"),
        (4,4,"Christian Lindner","FDP","Rhein-Sieg-Kreis II"),
        (5,5,"Alice Weidel","AfD","Bodensee"),
        (6,6,"Karl Lauterbach","SPD","Leverkusen-Köln IV"),
        (7,7,"Annalena Baerbock","Grüne","Potsdam"),
        (8,8,"Wolfgang Kubicki","FDP","Schleswig-Holstein Nord"),
        (9,9,"Peter Altmaier","CDU","Saarlouis"),
        (10,10,"Thomas Bareiß","CDU","Zollernalb-Sigmaringen"),
    ]
    c.executemany("INSERT OR IGNORE INTO abgeordnete (id,aw_id,name,partei,wahlkreis) VALUES (?,?,?,?,?)", abgeordnete)

    # Spenden
    if c.execute("SELECT COUNT(*) FROM parteispenden").fetchone()[0] == 0:
        for s in [
            ("Deutsche Bank AG","CDU",500000,"2023-01-15","Finanzsektor"),
            ("Allianz SE","CDU",350000,"2023-02-20","Versicherung"),
            ("BMW Group","FDP",200000,"2023-03-10","Automobilindustrie"),
            ("RWE AG","CDU",420000,"2023-04-05","Energiesektor"),
            ("Rheinmetall AG","CDU",250000,"2023-05-30","Rüstung"),
            ("E.ON SE","CDU",380000,"2023-08-25","Energiesektor"),
            ("Volkswagen AG","SPD",180000,"2023-03-25","Automobilindustrie"),
            ("Bayer AG","FDP",175000,"2023-07-20","Pharma"),
            ("Susanne Klatten","CDU",50001,"2024-09-03","Sonstige"),
        ]:
            c.execute("INSERT INTO parteispenden (spender,empfaenger,betrag_eur,datum,branche,quelle) VALUES (?,?,?,?,?,'demo')", s)

    # Abstimmungen
    if c.execute("SELECT COUNT(*) FROM abstimmungen").fetchone()[0] == 0:
        for a in [
            (1,1001,"Lieferkettengesetz Reform","2023-02-28","Wirtschaft"),
            (2,1002,"Autobahnprivatisierung","2023-04-15","Infrastruktur"),
            (3,1003,"Atomkraft Laufzeitverlängerung","2023-05-10","Energie"),
            (4,1004,"Rüstungsexporte Ukraine","2023-06-20","Verteidigung"),
            (5,1005,"Pharmadaten-Gesetz","2023-08-10","Gesundheit"),
            (6,1006,"Energiepreisbremse Verlängerung","2023-09-28","Energie"),
        ]:
            c.execute("INSERT INTO abstimmungen (id,aw_id,titel,datum,thema) VALUES (?,?,?,?,?)", a)
        tendenzen = {
            1:{"CDU":0.2,"SPD":0.7,"Grüne":0.9,"FDP":0.1},
            2:{"CDU":0.8,"SPD":0.3,"Grüne":0.1,"FDP":0.9},
            3:{"CDU":0.9,"SPD":0.4,"Grüne":0.05,"FDP":0.8},
            4:{"CDU":0.9,"SPD":0.8,"Grüne":0.7,"FDP":0.8},
            5:{"CDU":0.5,"SPD":0.6,"Grüne":0.4,"FDP":0.8},
            6:{"CDU":0.8,"SPD":0.7,"Grüne":0.5,"FDP":0.6},
        }
        for abg_id,_,_,partei,_ in abgeordnete:
            for abst_id in range(1,7):
                p = tendenzen.get(abst_id,{}).get(partei,0.5)
                votum = "ja" if random.random()<p else ("nein" if random.random()<0.8 else "enthalten")
                c.execute("INSERT INTO abstimmungs_ergebnis (abstimmung_id,abgeordneter_id,votum,partei) VALUES (?,?,?,?)",(abst_id,abg_id,votum,partei))

    # Demo-Nebeneinkünfte
    if c.execute("SELECT COUNT(*) FROM nebeneinkuenfte").fetchone()[0] == 0:
        demo_ne = [
            (1,1,1,"Blackrock Deutschland","Berater",7000,15000,"Finanzsektor","2021-01-01",""),
            (2,2,4,"BMW Aufsichtsrat","Aufsichtsratsmitglied",30000,50000,"Automobilindustrie","2021-01-01",""),
            (3,3,1,"RWE AG","Vortragstätigkeit",3500,7000,"Energiesektor","2022-03-01",""),
            (4,4,9,"Rheinmetall Beirat","Beiratsmitglied",50000,75000,"Rüstung","2021-06-01",""),
            (5,5,10,"E.ON Consulting","Beratungsvertrag",15000,30000,"Energiesektor","2022-01-01",""),
            (6,6,3,"Bayer AG","Vortrag","3500",7000,"Pharma","2023-01-01",""),
            (7,7,6,"GKV-Spitzenverband","Beirat",1000,3500,"Gesundheit","2021-01-01",""),
        ]
        for d in demo_ne:
            c.execute("INSERT OR IGNORE INTO nebeneinkuenfte (id,aw_id,abgeordneter_id,organisation,taetigkeit,betrag_min,betrag_max,branche,beginn,ende) VALUES (?,?,?,?,?,?,?,?,?,?)",d)

    # Demo-Ausschüsse
    if c.execute("SELECT COUNT(*) FROM ausschuesse").fetchone()[0] == 0:
        demo_au = [
            ("V001","Verteidigungsausschuss",9,"Mitglied","CDU"),
            ("V001","Verteidigungsausschuss",1,"Vorsitz","CDU"),
            ("E001","Ausschuss für Energie und Klimaschutz",3,"Mitglied","Grüne"),
            ("E001","Ausschuss für Energie und Klimaschutz",10,"Stellv. Vorsitz","CDU"),
            ("F001","Finanzausschuss",4,"Mitglied","FDP"),
            ("F001","Finanzausschuss",2,"Mitglied","SPD"),
            ("W001","Wirtschaftsausschuss",8,"Vorsitz","FDP"),
            ("G001","Gesundheitsausschuss",6,"Vorsitz","SPD"),
            ("G001","Gesundheitsausschuss",7,"Mitglied","Grüne"),
        ]
        c.executemany("INSERT INTO ausschuesse (ausschuss_id,ausschuss_name,abgeordneter_id,rolle,partei) VALUES (?,?,?,?,?)", demo_au)

    conn.commit()
    conn.close()
    print("  ✓ Demo-Daten geladen")


# ═══════════════════════════════════════════════════════════
# API-TEST
# ═══════════════════════════════════════════════════════════

def teste_apis():
    print("\n🔌 API-Verbindungstest:")
    tests = [
        ("Abgeordnetenwatch Abgeordnete", f"https://www.abgeordnetenwatch.de/api/v2/parliament-periods/{WAHLPERIODE_ID}"),
        ("Abgeordnetenwatch Sidejobs",    "https://www.abgeordnetenwatch.de/api/v2/sidejobs?pager_limit=1"),
        ("Bundestag Spenden",             "https://bundestag.api.proxy.bund.dev/parlament/praesidium/parteienfinanzierung/fundstellen50000/2024"),
        ("Bundestag Ausschüsse XML",      "https://www.bundestag.de/xml/v2/ausschuesse/index.xml"),
        ("DIP Bundestag",                 f"https://search.dip.bundestag.de/api/v1/person?apikey={DIP_API_KEY}&limit=1"),
    ]
    for name, url in tests:
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent","KorruptionsRadar/2.0")
            with urllib.request.urlopen(req, timeout=8) as r:
                print(f"  ✅ {name}: HTTP {r.status}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")


# ═══════════════════════════════════════════════════════════
# HAUPTPROGRAMM
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    modus = sys.argv[1] if len(sys.argv) > 1 else "--echt"
    print("\n🔍 KORRUPTIONS-RADAR v2\n" + "="*45)
    init_db()

    if modus == "--reset":
        print("  -> Loesche alte Daten...")
        conn = sqlite3.connect(DB_PATH)
        for t in ["super_korrelationen","drehtuer","korrelationen","abstimmungs_ergebnis",
                  "abstimmungen","nebeneinkuenfte","ausschuesse","parteispenden","abgeordnete"]:
            conn.execute(f"DELETE FROM {t}")
        conn.commit()
        conn.close()
        print("  Alte Daten geloescht")
        modus = "--echt"

    if modus == "--test":
        teste_apis()

    elif modus == "--demo":
        lade_demo_daten()
        analysiere_korrelationen()
        berechne_drehtuer()
        berechne_super_scores()
        print("\n Demo abgeschlossen.")

    else:
        print("\n Lade echte Daten...\n")
        print("[1/7] Abgeordnete...")
        lade_abgeordnete()
        print("\n[2/7] Parteispenden...")
        lade_parteispenden()
        print("\n[3/7] Abstimmungen...")
        lade_abstimmungen(max_seiten=10)
        print("\n[4/7] Abstimmungsergebnisse...")
        lade_abstimmungsergebnisse(max_abstimmungen=40)
        print("\n[5/7] Nebeneinkünfte...")
        lade_nebeneinkuenfte(max_seiten=20)
        print("\n[6/7] Ausschüsse...")
        lade_ausschuesse()

        conn = sqlite3.connect(DB_PATH)
        conn.close()

        print("\n[7/7] Analyse...")
        analysiere_korrelationen()
        berechne_drehtuer()
        berechne_super_scores()

        conn = sqlite3.connect(DB_PATH)
        print(f"""
📊 ERGEBNIS:
   Abgeordnete:           {conn.execute("SELECT COUNT(*) FROM abgeordnete").fetchone()[0]:>6}
   Parteispenden:         {conn.execute("SELECT COUNT(*) FROM parteispenden").fetchone()[0]:>6}
   Abstimmungen:          {conn.execute("SELECT COUNT(*) FROM abstimmungen").fetchone()[0]:>6}
   Nebeneinkünfte:        {conn.execute("SELECT COUNT(*) FROM nebeneinkuenfte").fetchone()[0]:>6}
   Ausschussmitgliedsch.: {conn.execute("SELECT COUNT(*) FROM ausschuesse").fetchone()[0]:>6}
   Drehtür-Verdächtige:   {conn.execute("SELECT COUNT(*) FROM drehtuer").fetchone()[0]:>6}
   Standard-Korrelationen:{conn.execute("SELECT COUNT(*) FROM korrelationen WHERE verdachts_score>=0.5").fetchone()[0]:>6}
   Super-Score Fälle:     {conn.execute("SELECT COUNT(*) FROM super_korrelationen WHERE super_score>=0.6").fetchone()[0]:>6}

✅ Dashboard: http://homeassistant.local:7755
        """)
        conn.close()
