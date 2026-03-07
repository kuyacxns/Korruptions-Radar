#!/usr/bin/with-contenv bashio

bashio::log.info "Starte Korruptions-Radar..."
mkdir -p /data/korruptions_radar/reports
mkdir -p /data/korruptions_radar/cache

# Flask ist nicht installiert - verwende eingebautes Python http.server
cd /usr/share/korruptions_radar
python3 -m http.server 7755

# Wöchentlichen Bericht generieren (jeden Montag 07:00)
(while true; do
  TAG=$(date +%u)  # 1=Mo, 7=So
  STUNDE=$(date +%H)
  if [ "$TAG" = "1" ] && [ "$STUNDE" = "07" ]; then
    bashio::log.info "Generiere Wochenbericht..."
    python3 /usr/bin/reporter_tools.py --weekly
    sleep 3600  # 1h warten damit es nicht nochmal triggert
  fi
  sleep 1800  # alle 30min prüfen
done) &

# Daten täglich um 03:00 Uhr neu laden
(while true; do
  STUNDE=$(date +%H)
  if [ "$STUNDE" = "03" ]; then
    bashio::log.info "Tägliche Datenaktualisierung..."
    python3 /usr/bin/daten_sammler.py
    sleep 3600
  fi
  sleep 1800
done) &

# Flask API starten
bashio::log.info "Starte API auf Port 7755..."
bashio::log.info "Landing Page: http://homeassistant.local:7755/"
bashio::log.info "Dashboard:    http://homeassistant.local:7755/dashboard"
bashio::log.info "CSV-Export:   http://homeassistant.local:7755/api/export/csv"
bashio::log.info "Report:       http://homeassistant.local:7755/api/export/report"

python3 /usr/bin/api_server.py
