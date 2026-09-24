# Corpusbuilder → Kantropos/RAG: Betriebsanleitung

Stand: 24.09.2026, gegen den aktuellen Code geprüft. Diese Anleitung verhindert
bekannte Bedienfehler, garantiert aber keine fehlerfreien PDFs. Bei Problemen
anhalten, Ergebnisse sichern und gezielt fortsetzen. Nicht Prüfungen umgehen.

**Standardweg:** Zuordnung prüfen → genau einen Entwurf erstellen → diesen
vorbereiten und prüfen → denselben Entwurf übernehmen → Markdown prüfen →
Embedding → Abschluss im Index/RAG nachweisen.

## 1. Zuständigkeiten und Reihenfolge

| Schritt | Zuständig | Ergebnis, noch keine Bestätigung des Gesamtlaufs |
|---|---|---|
| Download und Korpusauswahl | DT / Corpusbuilder, Justus' Code | PDFs und Metadaten sammeln, lokale Korpora einem RAG-Ziel zuordnen |
| Entwurf | DT | PDFs kopieren, Manifest und neue BibTeX-Fassung vorbereiten, Live-Korpus unverändert |
| Textprüfung und OCR | DT-Pipeline und OCR-Dienst | Schwache PDFs erkennen lassen, OCR-Texte neben den Entwurfs-PDFs sichern |
| Übernahme | DT | PDFs, OCR-Texte und geprüfte `metadata.bib` in den Zielkorpus übernehmen |
| Markdown/Textkonvertierung | Integral Learnings Kantropos `corpus-updater` | Aus PDFs bzw. OCR-Sidecars Textdateien in `markdown/` erzeugen |
| Vollständigkeitsprüfung | DT | Für jedes Entwurfs-PDF eine nichtleere Textdatei verlangen |
| Embedding und Index | Integral Learnings Kantropos | Texte aufteilen, Vektoren berechnen und neue Dokumente im Index speichern |

Die lokale Reparatur des Kantropos-Konverters liegt unter
[`backend/kantropos/`](../../backend/kantropos/README.md) im DT-Repo und wird in
den Updater eingebunden. Sie ist eine lokale Anpassung der **RAG-Komponente**,
keine Änderung im Repository von Integral Learning und kein Rechtmaschine-Fix.
Der separate [OCR-Dienst](../../backend/ocr/README.md) verwendet weiterhin eine
Legacy-Manager-Abhängigkeit. Diese ist nicht der Markdown-Konverter.

## 2. Vor jedem Lauf

### Im Browser: richtige Person und richtige Korpora

1. Am Corpusbuilder prüfen, mit welchem Konto man angemeldet ist.
2. Bei jedem einzubeziehenden lokalen Korpus die Kantropos-Zuordnung prüfen.
   Die Auswahl steht beim lokalen Korpus-Selektor im Kopfbereich.
3. Unerwünschte Korpora dürfen dem Ziel **nicht** zugeordnet sein. Eine Zuordnung
   entfernen löscht weder PDFs noch bereits importierte RAG-Daten.
4. Ein Lauf sammelt aus **allen** zu diesem Ziel zugeordneten Korpora, nicht nur
   aus dem gerade im Browser geöffneten Korpus.
5. Zuordnung während Entwurf/Import nicht ändern. Ein gespeicherter Entwurf ist
   eine Momentaufnahme und wird nicht nachträglich automatisch umgefiltert.
   Änderungen erfordern eine neue Prüfung seiner Auswahl.

Eine Zuordnung startet keine Verarbeitung. Die Upstream-Ansicht ersetzt nicht
den Server-Ablauf. Nur heruntergeladene, vorhandene PDFs können übernommen werden.

### Auf dem Produktionsserver, nicht im lokalen Terminal oder Container

Kim verwendet ihren bestehenden SSH-Zugang zu `148.251.184.244` und wechselt,
soweit bereits berechtigt, mit `sudo -iu spott` zum Betreiberkonto. Keine neuen
SSH-Benutzernamen ausprobieren und keine Zugangsdaten in Logs kopieren.

```bash
cd /home/spott/DT
git status --short
git rev-parse HEAD
docker ps --format '{{.Names}} {{.Status}}'
tmux list-sessions
```

`no server running` bei tmux bedeutet nur, dass keine tmux-Sitzung existiert.
Eine vorhandene Sitzung `dt-korpus` zuerst mit `tmux attach -t dt-korpus`
ansehen. Einen aktiven Auftrag **nicht zweimal starten**. Auch Logs prüfen:
Ein HTTP-Auftrag kann nach einem Client-Abbruch weiterlaufen.

Erwartet werden insbesondere `rag_feeder_backend`, `kantropos-corpus-updater`,
`kantropos-rag`, `kantropos-qdrant` und `kantropos-postgres`. Vor einem neuen Lauf
müssen Code und Laufzeit bereits bereit sein. Keine ungeprüften Updates,
Container-Neustarts oder Modellwechsel während laufender Verarbeitung.

**Rechte:** Benutzer, Pfade und Mounts prüfen, nicht `chmod -R 777` verwenden.
Der DT-Webcontainer sieht den Korpus absichtlich nur lesend. Die Übernahme nutzt
einen kurzlebigen Container mit Schreibrecht ausschließlich für das gewählte
Ziel. Fehlende Rechte gezielt vom Administrator korrigieren lassen.

**Ressourcen:** Entwurf und Live-Korpus brauchen Platz für zusätzliche PDFs,
Texte und Indexdaten. Mit `df -h /home/spott/DT /data/projects/kantropos` prüfen.
Vor OCR GPU-Auslastung mit `nvidia-smi` prüfen. Andere Modelle nicht eigenmächtig
beenden. Der OCR-Dienst verweigert Aufträge bei zu wenig freiem VRAM, reserviert
den Speicher aber nicht gegen später startende Fremdprozesse.

### Ist der Markdown-Fix aktiv?

```bash
docker inspect kantropos-corpus-updater \
  --format '{{range .Mounts}}{{println .Source "->" .Destination .RW}}{{end}}'
docker exec kantropos-corpus-updater python -B -c \
  'import util.markdown_util as m; print(hasattr(m, "require_success"))'
```

Erwartet: DT-Adapter auf `/corpus-updater/util/markdown_util.py`, `RW=false`,
zweiter Befehl `True`. Der Importtest prüft die verfügbare Datei, nicht den
Code eines alten laufenden Prozesses. Nach Codewechsel muss der Updater im
Leerlauf neu erstellt worden sein. Fehlt der Fix, Abschnitt 9 beachten.

## 3. Neuer Lauf: genau einen Entwurf erstellen

Erst wenn kein Auftrag mehr aktiv ist, eine geschützte Sitzung eröffnen:

```bash
tmux new -s dt-korpus
```

Die nächsten Befehle **in dieser Sitzung auf dem Host** ausführen. Sie laufen
im Vordergrund. Zum Trennen `Strg+B`, dann `D`, nicht `Strg+C` drücken.
Wieder verbinden: `tmux attach -t dt-korpus`.

```bash
cd /home/spott/DT
set -o pipefail
upstream=backend/scripts/kantropos_upstream.sh
export RAG_FEEDER_UPSTREAM_TARGET_ID=anthropozan-nachhaltiges-management
mkdir -p logs
bash "$upstream" count
```

Ziel, Korpusnamen und fehlende Dateien prüfen. Bei falscher Auswahl anhalten und
die Zuordnung korrigieren. Zählwerte sind Planungshilfe, kein Index-Nachweis.
Derselbe Work kann mehreren lokalen Korpora angehören, deshalb ist das Manifest
die maßgebliche Dateiliste. Für ein anderes Ziel dessen konfigurierte ID verwenden,
auch beim Wiederaufnehmen. Der Host-Lock wird nach dieser ID benannt.

```bash
bash "$upstream" draft
```

Den ausgegebenen **Container-Pfad** `draft_dir` aufheben. Jetzt nur den letzten
Ordnernamen aus dieser Ausgabe eingeben, keinen neuen Namen erfinden:

```bash
draft_base=/usr/src/app/dl_lit_project/data/upstream_update_drafts
read -r -p 'Ordnername aus draft_dir: ' draft_run
draft_dir="$draft_base/$draft_run"
docker exec rag_feeder_backend test -f "$draft_dir/manifest.json"
run_log="logs/kantropos-$draft_run"
```

Bei einem Fehler nicht fortfahren. Variablen in einer neuen Shell erneut setzen.
Der September-Lauf ist **kein** allgemeiner Vorgabewert.

| Verwendung | Pfad |
|---|---|
| Argument für `--draft-dir` | `/usr/src/app/dl_lit_project/data/upstream_update_drafts/<Lauf>` |
| Derselbe Entwurf auf dem Host | `/home/spott/DT/dl_lit_project/data/upstream_update_drafts/<Lauf>` |
| Live-Korpus auf Host und im DT-Backend | `/data/projects/kantropos/corpora/<Zielname>` |
| Live-Korpus im Integral-Learning-Updater | `/corpus-updater/corpora/<Zielname>` |

Das Manifest enthält Work-IDs, Quell-PDFs, Zielnamen und BibTeX-Keys. Daneben liegen
`metadata.current.bib` als Ausgangsstand, `metadata.pending-additions.bib`,
`metadata.bib.new` und Kopien unter `files/`. Ziel, Auswahl und Metadaten vor der
Freigabe ansehen. Nicht nur eine der zusammengehörigen Dateien manuell ändern.

## 4. Vorbereiten: Prüfung und gegebenenfalls OCR

```bash
bash "$upstream" validate "$draft_dir"
bash "$upstream" rag-flow --draft-dir "$draft_dir" \
  2>&1 | tee -a "$run_log.prepare.log"
```

Dies ist ein **Probelauf ohne Live-Import**, aber kein schreibfreier Test:
Textprüfung und echte OCR laufen, schreiben Ergebnisse im Entwurf und verbrauchen
Rechenzeit. Bei einem neuen Setup zuerst einen separaten kleinen Entwurf mit
`draft --limit 10` prüfen. Diesen nicht mit einem vollständigen Entwurf verwechseln.

1. Alle Entwurfs-PDFs auf auslesbaren Text prüfen, `text-scan.json` speichern.
2. OCR prüft erneut und bearbeitet schwache/fehlerhafte PDFs. Standard: unter
   500 Zeichen oder unter 25 Prozent Seiten mit Text. Vorhandene nichtleere
   OCR-Sidecars werden wiederverwendet.
3. OCR speichert `files/<PDF-Stamm>.txt` und die Zuordnung im Manifest. Neue
   OCR-Erfolge werden pro Dokument gesichert. Bei Fehlern bleiben erfolgreiche
   Ergebnisse erhalten. Fehlende Seiten oder leere Antworten sind kein Erfolg.
4. Die Import-Vorschau prüft Dateien, Metadaten und Textbereitschaft.
5. Erwartetes Ende: `Dry run only. Rerun with --draft-dir ... --yes ...`.

Eine Liste mit Titeln und Confidence-Werten bestätigt **nur die OCR-Stufe**.
Bei `STOP: stage ... failed` ist der Probelauf nicht erfolgreich abgeschlossen.

MuPDF-Warnungen nicht pauschal ignorieren. `ok_with_warnings` bedeutet ausreichend
auslesbaren Text, aber mögliche Inhalts-/Darstellungsprobleme. `low_text`,
`empty_text` und `error` erfordern OCR bzw. Untersuchung. Beschädigte PDFs nur
nach Prüfung reparieren, Original sichern, niemals Seiten entfernen, um eine
Prüfung zu bestehen. Ausreichende Zeichenzahl beweist keine Textqualität.

## 5. Freigeben und denselben Entwurf fertig verarbeiten

Nur nach erfolgreicher Vorbereitung und Prüfung:

```bash
bash "$upstream" rag-flow --draft-dir "$draft_dir" \
  --skip-ocr --yes 2>&1 | tee -a "$run_log.apply-embed.log"
```

`--skip-ocr` ist hier zulässig, weil die Vorbereitung erfolgreich war. Erneute
Textprüfung und Textbereitschaftskontrolle bleiben aktiv. Ist die OCR noch nicht
vollständig, Abschnitt 6 verwenden.

Der Befehl übernimmt PDFs, OCR-Sidecars und Metadaten, wartet auf Kantropos'
Markdown-Konvertierung, prüft **alle Entwurfs-Dokumente** auf nichtleere Texte
und startet erst dann Embedding mit `sync_mode=INSERT`. Er wartet auch auf die
HTTP-Antwort des Embedding-Dienstes.

**Nicht danach `rag-flow --yes` ohne `--draft-dir` aufrufen.** Das legt einen
neuen Entwurf an, statt den geprüften Stand fortzusetzen.

**Kopieren von Befehlen:** Ein `\` muss das letzte Zeichen der Zeile sein.
Keine Leerzeile nach dem `\`, keine zerbrochenen Pfade. Die komplette Gruppe
kopieren. Niemals `--draft-dir` ohne unmittelbar folgenden Wert ausführen.

### Fortschritt

In einem zweiten Host-Terminal:

```bash
docker logs --since 10m --tail 100 -f kantropos-corpus-updater
```

`Strg+C` beendet dort nur die Loganzeige. Im Terminal des Auftrags unterbricht
es dessen Steuerung. Textprüfung, OCR und Übernahme haben eigene Zähler und
Heartbeats. Der Wrapper meldet bei HTTP-Aufrufen alle 30 Sekunden `Still waiting`.
Das ist weder eine ETA noch ein Beweis, dass der Server gesund ist.

Der Adapter meldet `start`, `progress`, `waiting`, `file_complete`, `file_failed`
und `complete`. Er nutzt standardmäßig höchstens vier CPU-Prozesse, keine OCR/GPU.
Embedding-Fortschritt stammt separat aus Kantropos. Große Dokumente und Korpora
können lange dauern. Nicht wegen einer Pause neu starten.

## 6. Wiederaufnahme nach Abbruch

Gespeicherten Entwurf, Phase und Fehler feststellen. tmux, Wrapper-Log und
Updater-/OCR-Logs prüfen. Ein fehlender tmux-Prozess oder freier Host-Lock beweist
nach einem Verbindungsabbruch nicht, dass kein HTTP-Auftrag mehr läuft. Während
aktiver Verarbeitung weder neu starten noch deployen.

**OCR unvollständig, keine aktive Restverarbeitung mehr:**

```bash
bash "$upstream" rag-flow --draft-dir "$draft_dir" \
  --yes 2>&1 | tee -a "$run_log.resume.log"
```

Erneute Prüfung, Wiederverwendung erfolgreicher OCR-Texte, Fortsetzung des Imports.
Für gezielte OCR-Diagnose kann `ocr "$draft_dir" --work-id <ID> --keep-going`
verwendet werden. Platzhalter ersetzen. Danach den vollständigen Entwurf prüfen.
Keine `--overwrite`-Option ohne Entscheidung über vorhandene Ergebnisse verwenden.
OCR-Client-Timeout standardmäßig 3660 Sekunden, Manager separat 3600 Sekunden.
Ein Timeout beweist nicht, dass die serverseitige Berechnung beendet ist.

**OCR fertig, Import/Markdown unvollständig:** Befehl aus Abschnitt 5 nutzen.
Passende PDFs/Sidecars bleiben erhalten. Abweichende vorhandene PDFs oder seitdem
geänderte Live-Metadaten führen zum Abbruch, nicht zum stillen Überschreiben.

**Import und Texte fertig, nur Embedding fehlt:** Nur nach bestätigtem Ende des
vorherigen Serverauftrags und Klärung eines eventuellen Teilimports:

```bash
bash "$upstream" rag-flow --draft-dir "$draft_dir" \
  --skip-ocr --skip-apply --skip-markdown --yes \
  2>&1 | tee -a "$run_log.embedding-resume.log"
```

Textprüfung, Übereinstimmung importierter Dateien/Metadaten und Textvollständigkeit
bleiben aktiv. `--yes` ist auch hier nötig. `INSERT` lässt bestehende Dokument-IDs
aus. **Ein teilweise eingebettetes Dokument kann bereits eine ID besitzen und
dadurch beim Retry übersprungen werden.** Nach Embedding-Abbruch deshalb betroffene
Dokumente/Chunks prüfen. Nicht blind `UPSERT` oder `REPLACE` verwenden: Das kann
vorhandene Daten verändern oder eine Sammlung ersetzen.

Auch Vektor-IDs und PostgreSQL-Metadaten müssen zusammenpassen: Der Updater
speichert Vektoren vor dem Metadaten-Commit. Scheitert danach das Speichern einer
Autorenangabe, überspringt `INSERT` das Dokument beim nächsten Versuch trotzdem.
Für die verlustfreie Erweiterung des Autorenfelds und die gezielte Nachpflege
solcher Metadaten siehe [Datenbankmigration und Wiederaufnahme](../../backend/kantropos/migrations/README.md).

## 7. Fehler erkennen und richtig reagieren

| Meldung / Symptom | Bedeutung | Nächster Schritt |
|---|---|---|
| `Missing value for --draft-dir`, danach `command not found` | Befehl beim Kopieren zerlegt | Variablen und vollständigen mehrzeiligen Block verwenden |
| `No such file or directory` beim Entwurf | Falscher Pfad oder fehlender Entwurf | Pfadtabelle und Manifest prüfen, keinen neuen Entwurf als Ersatz anlegen |
| `Permission denied` | Benutzer/Mount/Zielrechte falsch | Gezielte Rechte prüfen, keine pauschale Freigabe |
| `Live metadata changed` / `different content` | Entwurf passt nicht mehr zum Live-Stand | Änderungen abgleichen, nicht überschreiben |
| OCR HTTP 503 / GPU busy | Zu wenig Grafikspeicher | Andere laufende Arbeit berücksichtigen, später fortsetzen |
| OCR-/HTTP-Timeout | Client wartet nicht mehr, Server eventuell schon | Status/Logs prüfen, kein automatischer Doppelversuch |
| `Markdown coverage incomplete` | Entwurfs-Text fehlt oder ist leer | Updater-Version und betroffene Dateien prüfen, Embedding nicht erzwingen |
| `UnicodeEncodeError` / surrogates | Konverter erzeugt ungültige Unicode-Zeichen | Adapter prüfen, betroffene Texte gezielt reparieren |
| `StringDataRightTruncation` / `character varying(255)` beim Autor | Autorenangabe überschreitet die DB-Feldgrenze | Backup, Autorenfeld-Migration und Vektor/Metadaten-Abgleich gemäß Migrationsanleitung, nicht kürzen |
| HTTP-Erfolg, aber fehlende Dokumente | HTTP-Status beweist keine Vollständigkeit | Manifest gegen Textdateien und Index vergleichen |
| Pending-Anzeige bleibt hoch | DTs importierte Baseline kann veraltet sein | DT-Importstand getrennt prüfen, keinen Lauf allein wegen der Anzeige starten |

**Markdown-Vorfall vom 22.09.2026:** 47 von 21.132 Textdateien waren leer. Drei
Dateien scheiterten beim UTF-8-Schreiben, bei weiteren PDFs lieferte die
Layout-Konvertierung leeren Text trotz vorhandener Textebene. Der alte Konverter
meldete trotzdem Erfolg und übersprang vorhandene leere Dateien. DTs Kontrolle
blockierte Embedding korrekt. Der Adapter versucht leere/ungültige Ausgaben erneut,
nutzt nötigenfalls die PDF-Textebene, protokolliert ersetzte ungültige Zeichen und
gibt verbleibende Dateifehler als Fehler weiter.

Gezielte Reparatur: [`repair_empty.py`](../../backend/kantropos/repair_empty.py),
nur für bereits importierte PDFs mit nutzbarer Textebene:

- Manifest und Skript im Updater bereitstellen, mit `PYTHONPATH=/corpus-updater` ausführen.
- Mit `--manifest <Datei> --expected-count <geprüfte Anzahl>` erst Vorschau ausführen.
- Bei genau passender Auswahl `--yes` ergänzen. Keine parallele Verarbeitung.
- Alte Ausgaben werden unter `.dt-markdown-backup-<Zeitstempel>` im Korpus
  gesichert, dort entsteht `report.json`. Gute Texte und PDFs bleiben unangetastet.
- Das Werkzeug startet weder OCR noch Embedding und repariert keine defekten PDFs.
- Danach vollständige Textprüfung und kontrollierte Wiederaufnahme oben.

## 8. Wann ist der Lauf wirklich fertig?

Alle folgenden Kriterien müssen passen:

1. Wrapper beendet ohne Fehler/`STOP`, mit `Kantropos embedding request succeeded`.
2. Updater meldet Embedding-Abschluss und erfolgreiche HTTP-Antwort, nicht nur
   `Start embedding` oder eingelesene Dateinamen.
3. Alle Manifest-Einträge haben nichtleere Texte. Auf dem Host:

   ```bash
   bash "$upstream" check-markdown --draft-dir "$draft_dir"
   ```

   Erwartet: `checked` entspricht der Manifest-Anzahl, `missing_or_empty` ist `[]`.
4. Im richtigen Qdrant-Korpus **mit dem konfigurierten Embedding-Modell** prüfen,
   dass die neuen Dokument-IDs (`ref_doc_id` = Ziel-PDF-Dateiname) vertreten sind.
   Nach einem abgebrochenen Vorlauf auch Chunk-Vollständigkeit prüfen. Vektoranzahl
   ist nicht Dokumentanzahl. Zusätzlich für alle Manifest-Dokumente die passenden
   PostgreSQL-Metadaten im richtigen Korpus prüfen. Vektor-IDs allein reichen nicht.
5. Im RAG mehrere eindeutig neue Dokumente über charakteristische Inhalte abfragen
   und Quellen prüfen, darunter reparierte/OCR-Dokumente.
6. Laufordner, Codeversion, Ziel, Anzahl, Logpfade und Beispiele notieren. Erst dann
   „abgeschlossen“ melden. Ein Einzelbeispiel beweist nicht die Gesamtvollständigkeit.

Nichtleerer Text beweist weder korrektes Layout noch vollständige Seiten oder
passende Metadaten. Stichproben bleiben nötig. OCR-Confidence bewertet nur OCR.

DTs Pending-Ansicht nutzt eine lokale importierte Baseline (`origin_type =
bibtex_import`, `origin_key` = Ziel-`metadata.bib`). Ihre Aktualisierung und
Vektorisierung sind getrennte Vorgänge. `apply` aktualisiert Korpusdateien, nicht
automatisch diese lokale Baseline. Der Scraper-Export ist ein eigener Workflow.

## 9. Administratoren: Deployment, Sicherung, Grenzen

RAG-Stack: `/data/projects/kantropos`. Persistenter Adapter-Mount:
`compose.production-uol.yml`, `services.corpus-updater.volumes`. Details und
Rollback: [`backend/kantropos/README.md`](../../backend/kantropos/README.md).

Nur im Leerlauf, nach Sicherung der Konfiguration und Prüfung des Änderungsumfangs:

```bash
cd /data/projects/kantropos
docker compose \
  -f compose.yml \
  -f compose.staging.yml \
  -f compose.production.yml \
  -f compose.production-uol.yml \
  up -d --no-deps --pull never corpus-updater
```

Das ist **kein Schritt jedes Korpuslaufs**. Nicht den gesamten Stack neu erstellen.
Bei Compose-Variablenwarnungen die betroffene Dienstkonfiguration gegen den
laufenden Stand prüfen, ohne Geheimnisse auszugeben. Bei Image-Updates die
Kompatibilität des lokal eingebundenen Adapters erneut testen.

Originale, Manifest, OCR-Sidecars, Metadaten-Backups und Logs aufbewahren. `apply`
sichert `metadata.bib`, ist aber keine vollständige Transaktion über Dateien,
PostgreSQL und Qdrant. Ein altes `metadata.bib` zurückkopieren macht Vektoren nicht
rückgängig. Code-Rollback und Daten-Rollback sind getrennte Aufgaben.

Direkte POST-Aufrufe und die Ausgabe von `commands` sind technische Hilfen,
**nicht der sichere Standardweg**: Sie umgehen teilweise Host-Lock, Import- und
Textprüfungen. Keine Hintergrund-`curl ... &`-Ketten für den Normalbetrieb.
Der Lock schützt passende Wrapper-Aufrufe auf demselben Host mit derselben Ziel-ID,
nicht beliebige direkte API-Aufrufe oder andere Rechner.

`INSERT` ist für neue Dokumente. Geänderte bereits eingebettete PDFs, korrigierte
Retrieval-Metadaten, neue Embedding-Modelle oder Löschung bereits importierter
Dokumente brauchen einen gesonderten Updateplan. Nicht einfach Textdateien löschen
oder eine Collection ersetzen.

## 10. Prüfung dieser Anleitung

Maßgebliche Implementierung:

- [`kantropos_upstream.sh`](../../backend/scripts/kantropos_upstream.sh): Reihenfolge, Locks, Optionen, HTTP-Warteverhalten.
- [`upstream_update.py`](../../backend/scripts/upstream_update.py): Entwürfe, Scans, OCR, Import- und Textprüfungen.
- [`markdown_util.py`](../../backend/kantropos/markdown_util.py): lokaler RAG-Konverter-Adapter.
- [`OCR-Profil`](../../backend/ocr/README.md): Laufzeit, Timeout, GPU-Schutz.

Regressionstests ohne Produktionslauf:

```bash
python3 -m unittest discover -s dl_lit_project/tests \
  -p 'test_upstream*.py'
python3 -m unittest discover -s dl_lit_project/tests \
  -p 'test_kantropos_markdown.py'
```

Diese Anleitung ersetzt ältere Mail-Befehle und ungesicherte Einzelaufrufe.
Sie bestätigt keinen noch laufenden Auftrag als abgeschlossen.
