-- Databricks notebook source
-- DBTITLE 1,Einleitung
-- MAGIC %md
-- MAGIC # Case Study – Kennzahl „wasted Picking Effort“
-- MAGIC
-- MAGIC ## Ausgangssituation
-- MAGIC Bei der flaschenpost arbeiten wir auf Databricks mit einer **Data-Mesh-Architektur**. Zentral angebundene (Roh-)Daten stehen allen Fachbereichen zur Verfügung und können dort eigenständig verfeinert, angereichert und analysiert werden.
-- MAGIC
-- MAGIC Auf Databricks liegen die Rohdaten im Katalog **`flaschenpost`**: im Schema **`commercetools`** die Rohdaten aus commercetools, im Schema **`events`** die Daten unseres Event Workers. Andere Fachbereiche haben jeweils einen eigenen Katalog und können ihre eigenen Datenprodukte dort ablegen.
-- MAGIC
-- MAGIC Es ist aufgefallen, dass ein Power-BI-Report des Fachbereichs **`warehousing`** direkt die Tabellen aus dem Katalog `flaschenpost` abfragt. Eine dieser Abfragen ermittelt die Kennzahl **„wasted Picking Effort“**: Die Anzahl der Bestellungen, die storniert wurden, obwohl im Lager bereits Kommissionieraufwand angefallen ist.
-- MAGIC
-- MAGIC ## Aufgabenstellung
-- MAGIC Die SQL-Abfrage, wie Power BI sie an Databricks übermittelt, findest du unten in der Zelle **„Power BI-Abfrage“**. Wir sehen bei allen, insbesondere aber bei dieser Abfrage Handlungsbedarf. Bitte analysiere die Abfrage und präsentiere dem Fachbereich, welche Möglichkeiten zur Optimierung du identifiziert hast und wie mögliche zukünftige Probleme vermieden werden können. Orientiere dich an diesen Fragen:
-- MAGIC
-- MAGIC 1. Welche Probleme siehst du bei der Abfrage und bei der direkten Verwendung der Quelldaten (`commercetools`-/`events`-Tabellen) im Report?
-- MAGIC 2. a. Wie würdest du die Daten in der Medallion-Architektur strukturieren (Entitäten und Granularität)?<br>b. Wie und wo würdest du die Daten im Katalog ablegen?
-- MAGIC 3. Wie würdest du die Datenstrecke funktional aufbauen und mit neuen Daten versorgen?
-- MAGIC 4. Welche Ideen hast du, wie die Quelldaten noch optimiert werden könnten?
-- MAGIC
-- MAGIC Sollten dir Informationen fehlen, ergänze gern Annahmen und dokumentiere sie unten.
-- MAGIC
-- MAGIC ## Rahmen
-- MAGIC | | |
-- MAGIC | --- | --- |
-- MAGIC | Voraussetzung | `01_Setup_Tabellen_anlegen` erfolgreich ausgeführt (alle Tabellen `ok = true`) |
-- MAGIC | Compute | **Serverless Starter SQL Warehouse** (oben rechts als Compute auswählen) |
-- MAGIC | Sprache | SQL (auf SQL Warehouses laufen keine Python-Zellen \| oben links in der Menüleiste ggf. von `Python` auf `SQL` umstellen) |
-- MAGIC | Daten | In deiner Umgebung **statisch** (Bestellungen September 2026, Events bis 3. Oktober 2026). In der Realität kommen **minütlich** neue Daten hinzu. |
-- MAGIC
-- MAGIC **Hinweise**
-- MAGIC * Zuerst die Zelle **Setup** ausführen – nach jedem Neuverbinden des Warehouses erneut.
-- MAGIC * Die Zelle **Power BI-Abfrage** bitte nicht verändern; sie ist die Ausgangsbasis für deine Analyse.
-- MAGIC * Du darfst eigene Kataloge, Schemas, Tabellen und Views anlegen und weitere Zellen ergänzen. 
-- MAGIC * Ebenfalls kannst du eigene Notebooks anlegen die dir bei der Bearbeitung helfen könnten.
-- MAGIC * Du musst deine bearbeiteten (oder neu angelegten) Notebooks nicht abgeben. Die Abgabe deiner Inhalte für die Präsentation sind ausreichend.

-- COMMAND ----------

-- DBTITLE 1,Setup (nicht ändern)
-- Nach jedem Neuverbinden des Warehouses erneut ausfuehren
USE CATALOG flaschenpost;
-- Ergebnis-Cache aus, damit wiederholte Abfragen echte Laufzeiten zeigen
SET use_cached_result = false;
-- Query Tags: Abfragen dieser Session sind in system.query.history erkennbar
SET QUERY_TAGS['bic_case'] = 'wasted_picking_effort';
SELECT current_catalog() AS catalog, current_user() AS benutzer,
       (SELECT count(*) FROM commercetools.variant_orders) AS variant_orders,
       (SELECT count(*) FROM commercetools.variant_states) AS variant_states,
       (SELECT count(*) FROM events.fulfillmentordercreated) AS fulfillmentordercreated,
       (SELECT count(*) FROM events.stockitemspicked) AS stockitemspicked

-- COMMAND ----------

-- DBTITLE 1,Datenmodell
-- MAGIC %md
-- MAGIC ## Datenmodell der Quelldaten
-- MAGIC
-- MAGIC | Tabelle | Inhalt | Grain | Wichtige Spalten |
-- MAGIC | --- | --- | --- | --- |
-- MAGIC | `flaschenpost.commercetools.variant_orders` | commercetools-Bestellungen, **eine Zeile je Version** | Bestellung × Version | `id`, `version`, `versionModifiedAt` (Text, UTC), `orderState`, `state` (VARIANT, Custom State), `lineItems` (VARIANT-Array), `custom` (VARIANT, u. a. `fields.FulfilmentWarehouseId`, `fields.StateCancelledSetAt`), `_bronze_created_at` (Ingestion) |
-- MAGIC | `flaschenpost.commercetools.variant_states` | Statusdefinitionen (32 Zeilen) | Status | `id`, `key` (Name), `type` (`OrderState`/`LineItemState`) |
-- MAGIC | `flaschenpost.events.fulfillmentordercreated` | Lagerauftrag (Fulfillment Order, FO) angelegt | FO | `FulfillmentOrderId`, `CoToOrderId` (= `variant_orders.id`), `LegacyOrderId` (= `orderNumber`), `HubId`, `Items` (VARIANT-Array `{ArticleId, ItemId}`, **ein Item je Stück**), `Timestamp` |
-- MAGIC | `flaschenpost.events.stockitemspicked` | Artikel gepickt | Pick-Event | `_id`, `FulfillmentOrderItemIds` (VARIANT-Array von `ItemId`), `ArticleId` (= SKU), `RequestedQuantity`, `PickedQuantity`, `Timestamp`, `_bronze_created_at` (Ingestion) |
-- MAGIC
-- MAGIC **Verknüpfung:** Bestellung `id` → FO `CoToOrderId` → FO-Items `Items[].ItemId` → Pick `FulfillmentOrderItemIds[]`.
-- MAGIC
-- MAGIC **Fachliche Hinweise**
-- MAGIC * Ein **Custom State** steht in `state.id` und wird über `variant_states.id` zum Namen (`key`) aufgelöst.
-- MAGIC * **Storniert** ist eine Bestellung, wenn in der maßgeblichen Version `orderState = 'Cancelled'` **oder** der Custom State `Cancelled` ist. Der Stornozeitpunkt steht in `custom.fields.StateCancelledSetAt[0]`.
-- MAGIC * Die Tabellen werden laufend ergänzt (Append): neue Bestellversionen und neue Events kommen minütlich hinzu, Events teils **stark verspätet** oder **technisch doppelt** (neue `_id`, sonst identischer Inhalt).
-- MAGIC * Zeitstempel liegen als Text vor, Event-Zeitstempel mit Zeitzonen-Offset.

-- COMMAND ----------

-- DBTITLE 1,Power BI-Abfrage (Beschreibung)
-- MAGIC %md
-- MAGIC ## Power BI-Abfrage „wasted Picking Effort“
-- MAGIC
-- MAGIC Der Report des Fachbereichs `warehousing` nutzt **DirectQuery**: Die folgende Abfrage wird bei jedem Öffnen, Filtern und Aktualisieren des Reports an das SQL Warehouse geschickt. Sie liefert je Hub:
-- MAGIC
-- MAGIC | Spalte | Bedeutung laut Fachbereich |
-- MAGIC | --- | --- |
-- MAGIC | `hub_id` | Hub |
-- MAGIC | `n_orders_cancelled` | stornierte Bestellungen |
-- MAGIC | `n_orders_cancelled_with_picks` | davon mit Kommissionieraufwand = **wasted Picking Effort** |
-- MAGIC | `units_picked` | gepickte Stück dieser Bestellungen |
-- MAGIC | `units_picked_after_cancel` | davon nach dem Stornozeitpunkt gepickt |
-- MAGIC
-- MAGIC Führe die Abfrage aus und sieh dir Laufzeit, Ergebnis und Query Profile an. Das Query Profile kannst du via 'See Performance' am Ende der Zelle über der Ausgabe anschauen.

-- COMMAND ----------

-- DBTITLE 1,Power BI-Abfrage (nicht ändern)
select `OTBL`.`hub_id`, `OTBL`.`n_orders_cancelled`, `OTBL`.`n_orders_cancelled_with_picks`,
       `OTBL`.`units_picked`, `OTBL`.`units_picked_after_cancel`
from (
WITH order_status AS (
  WITH v AS (
    SELECT id, orderNumber, version, orderState,
           variant_get(state, '$.id', 'string') AS state_id,
           variant_get(custom, '$.fields.FulfilmentWarehouseId', 'int') AS hub_id
    FROM flaschenpost.commercetools.variant_orders
    WHERE versionModifiedAt <= '2026-10-04T00:00:00.000Z'
    QUALIFY row_number() OVER (PARTITION BY id ORDER BY version DESC) = 1
  )
  SELECT v.id AS order_id, v.orderNumber AS order_number, v.version, v.orderState AS order_state, s.key AS custom_state, v.hub_id,
         v.orderState = 'Cancelled' OR coalesce(s.key = 'Cancelled', false) AS is_cancelled
  FROM v LEFT JOIN flaschenpost.commercetools.variant_states s ON s.id = v.state_id
),
picks_clean AS (
  SELECT _id AS pick_event_id, HubId AS hub_id, ArticleId, RequestedQuantity, PickedQuantity,
         to_timestamp(Timestamp) AS pick_ts, _bronze_created_at, FulfillmentOrderItemIds AS item_ids
  FROM flaschenpost.events.stockitemspicked
  QUALIFY row_number() OVER (PARTITION BY variant_get(FulfillmentOrderItemIds, '$[0]', 'string'), Timestamp
                             ORDER BY _bronze_created_at, _id) = 1
),
fo_items AS (
  SELECT f.FulfillmentOrderId AS fo_id, f.CoToOrderId, f.HubId AS hub_id, to_timestamp(f.Timestamp) AS fo_ts,
         variant_get(i.value, '$.ItemId', 'string') AS item_id
  FROM flaschenpost.events.fulfillmentordercreated f, LATERAL variant_explode(f.Items) i
),
pick_fo AS (
  SELECT DISTINCT p.pick_event_id, fi.fo_id, fi.CoToOrderId, fi.hub_id, CAST(p.ArticleId AS STRING) AS sku,
         p.RequestedQuantity, p.PickedQuantity, p.pick_ts, p._bronze_created_at
  FROM picks_clean p, LATERAL variant_explode(p.item_ids) x
  JOIN fo_items fi ON fi.item_id = variant_get(x.value, '$', 'string')
),
latest_variant_orders AS (
  SELECT id, to_timestamp(variant_get(custom, '$.fields.StateCancelledSetAt[0]', 'string')) AS cancel_ts
  FROM flaschenpost.commercetools.variant_orders
  WHERE versionModifiedAt <= '2026-10-04T00:00:00.000Z'
    AND id IN (SELECT order_id FROM order_status WHERE is_cancelled)
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY version DESC) = 1
),
cancelled_orders AS (
  SELECT st.order_id, st.hub_id, latest_variant_orders.cancel_ts
  FROM order_status st JOIN latest_variant_orders ON latest_variant_orders.id = st.order_id
  WHERE st.is_cancelled
)
SELECT c.hub_id,
       count(DISTINCT c.order_id) AS n_orders_cancelled,
       count(DISTINCT p.CoToOrderId) AS n_orders_cancelled_with_picks,
       coalesce(sum(p.PickedQuantity), 0) AS units_picked,
       coalesce(sum(CASE WHEN p.pick_ts > c.cancel_ts THEN p.PickedQuantity ELSE 0 END), 0) AS units_picked_after_cancel
FROM cancelled_orders c
LEFT JOIN pick_fo p ON p.CoToOrderId = c.order_id
GROUP BY c.hub_id
) `OTBL`
limit 1000001
