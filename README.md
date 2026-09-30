# Case Study – Bestellungen und Picking abgleichen

In dieser Case Study analysierst du eine Power-BI-Abfrage für die Kennzahl **„wasted Picking Effort“**, die Bestellungen aus **commercetools** mit **Picking-Events des Lagers** verknüpft, und entwickelst Vorschläge für Optimierung und Datenarchitektur.

Alle Daten sind **vollständig synthetisch**. Sie folgen den Schemas echter Quellsysteme, enthalten aber keine echten Kunden-, Bestell- oder Mitarbeiterdaten.

## Inhalt

| Pfad | Zweck | Compute |
| --- | --- | --- |
| `01_Setup_Tabellen_anlegen` | legt die Daten aus `data/` als Delta-Tabellen im Catalog `flaschenpost` an (Catalog wird bei Bedarf angelegt) und prüft sie | Serverless (Umgebung 4+) oder DBR 17.3 LTS+ |
| `02_Case_Study_Aufgaben` | Aufgabenstellung, Power-BI-Abfrage und Antwortzellen, nur SQL | **SQL Warehouse** |
| `data/<data_dir>/part-NNNNN.parquet` | Rohdaten, eine Datei je Ingestion-Zeitfenster (6 h) | |
| `data/manifest.json` | Ziel-Catalog, Ziel-Schema und -Tabelle, Spaltenschemas, Zeilen und SHA-256 je Datei, Inhalts-Fingerprint je Tabelle | |

## Schnellstart

1. Repo in Databricks als **Git-Ordner** klonen (Workspace → *Create* → *Git folder*).
2. `01_Setup_Tabellen_anlegen` mit **Serverless** öffnen und **Run all** (keine Parameter nötig). Alle Tabellen müssen am Ende `ok = true` zeigen.
3. `02_Case_Study_Aufgaben` öffnen, als Compute ein **SQL Warehouse** wählen, zuerst die Zelle *Setup* ausführen und die Aufgaben bearbeiten.

**Voraussetzungen:** Unity Catalog mit Rechten zum Anlegen der Schemas `commercetools` und `events` im Catalog `flaschenpost` (fehlt der Catalog, zusätzlich `CREATE CATALOG`); Zugriff auf ein Serverless SQL Warehouse; für die Auswertung aus der Query History Leserechte auf `system.query.history`.

## Daten

| Tabelle | Vorbild | Zeilen | Dateien |
| --- | --- | ---: | ---: |
| `flaschenpost.commercetools.variant_orders` | commercetools Order-Historie (eine Zeile je Version, VARIANT-Spalten) | 555.160 | 126 |
| `flaschenpost.commercetools.variant_states` | commercetools Statusdefinitionen | 32 | 1 |
| `flaschenpost.events.fulfillmentordercreated` | Lager: Fulfillment Order angelegt | 75.984 | 126 |
| `flaschenpost.events.stockitemspicked` | Lager: Artikel gepickt | 612.386 | 129 |

80.000 Bestellungen im September 2026, 5 Hubs, Events bis 3. Oktober 2026.

Im Parquet liegen `VARIANT`-Spalten als JSON-Text. Das Setup-Notebook wandelt sie beim Laden in `VARIANT` um.

Die Daten sind in deiner Umgebung statisch; in der Realität werden die Quelltabellen minütlich ergänzt.
