# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Anleitung
# MAGIC %md
# MAGIC # Case Study – Schritt 1: Tabellen anlegen
# MAGIC
# MAGIC Dieses Notebook legt die Daten der Case Study aus dem Ordner `data/` als Delta-Tabellen im Catalog **`flaschenpost`** an:
# MAGIC
# MAGIC | Tabelle | Inhalt |
# MAGIC | --- | --- |
# MAGIC | `flaschenpost.commercetools.variant_orders` | commercetools-Bestellungen (eine Zeile je Version) |
# MAGIC | `flaschenpost.commercetools.variant_states` | commercetools-Statusdefinitionen |
# MAGIC | `flaschenpost.events.fulfillmentordercreated` | Lager: Fulfillment Order angelegt |
# MAGIC | `flaschenpost.events.stockitemspicked` | Lager: Artikel gepickt |
# MAGIC
# MAGIC Gibt es den Catalog `flaschenpost` noch nicht, wird er angelegt (dafür ist das Recht `CREATE CATALOG` nötig).
# MAGIC
# MAGIC **Voraussetzungen**
# MAGIC * Dieses Repo ist als **Git-Ordner** in eurem Databricks-Workspace geklont.
# MAGIC * Compute für **dieses** Notebook: **Serverless** (Umgebung 4 oder neuer) oder ein Cluster mit **Databricks Runtime 17.3 LTS oder neuer**. Die Bearbeitung (`02_Case_Study_Aufgaben`) läuft danach auf einem **SQL Warehouse**.
# MAGIC * Ihr dürft im Catalog `flaschenpost` die Schemas `commercetools` und `events` sowie Tabellen anlegen (bzw. den Catalog selbst anlegen, falls er fehlt).
# MAGIC
# MAGIC **Bedienung**
# MAGIC 1. Keine Parameter nötig (`data_dir` nur setzen, wenn die Daten nicht in `./data` liegen).
# MAGIC 2. **Run all**. Dauer je nach Compute einige Minuten.
# MAGIC 3. Am Ende muss jede Tabelle in der Kontrolle `ok = true` zeigen.
# MAGIC
# MAGIC **Was das Notebook garantiert**
# MAGIC * Jede Parquet-Datei wird vor dem Laden per SHA-256 gegen `data/manifest.json` geprüft.
# MAGIC * Jede Parquet-Datei wird zu **genau einer** Delta-Datei, Zeilenreihenfolge unverändert.
# MAGIC * Die Tabellen werden **bewusst unoptimiert** angelegt: kein Variant Shredding, kein Clustering, keine automatische Komprimierung.
# MAGIC * Nach dem Laden werden Zeilenzahl, Dateianzahl und ein Inhalts-Fingerprint gegen das Manifest geprüft.
# MAGIC
# MAGIC > Bitte **kein** `OPTIMIZE`, `REORG` oder `ALTER TABLE` ausführen, bevor die Aufgaben das verlangen. Für einen Neustart dieses Notebook einfach erneut ausführen.

# COMMAND ----------

# DBTITLE 1,Parameter
import json, os, re, hashlib, time
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
spark.conf.set("spark.sql.session.timeZone", "UTC")

dbutils.widgets.text("data_dir", "", "Datenordner (leer = ./data neben diesem Notebook)")

DATA_DIR = re.sub(r"^file:", "", dbutils.widgets.get("data_dir").strip()).rstrip("/")
if not DATA_DIR:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    DATA_DIR = os.path.dirname(nb if nb.startswith("/Workspace") else "/Workspace" + nb) + "/data"

MANIFEST_PATH = f"{DATA_DIR}/manifest.json"
assert os.path.isfile(MANIFEST_PATH), f"Manifest nicht gefunden: {MANIFEST_PATH}"

with open(MANIFEST_PATH) as fh:
    MANIFEST = json.load(fh)

# Ziel: fester Catalog, Schema und Tabellenname je Tabelle aus dem Manifest
CATALOG = MANIFEST["catalog"]                                   # flaschenpost
SCHEMAS = sorted({m["schema"] for m in MANIFEST["tables"].values()})   # commercetools, events
IDENT = re.compile(r"^[A-Za-z0-9_\-]+$")
assert all(IDENT.match(x) for x in [CATALOG, *SCHEMAS, *(m["table"] for m in MANIFEST["tables"].values())]), "ungültiger Name im Manifest"
def fq(m): return f"`{CATALOG}`.`{m['schema']}`.`{m['table']}`"

print(f"Ziel-Catalog: {CATALOG} (Schemas: {', '.join(SCHEMAS)})\nDaten: {DATA_DIR}\nDatensatz: {MANIFEST['dataset_version']} (Seed {MANIFEST['seed']}, {MANIFEST['n_orders']:,} Bestellungen)")
for t, m in MANIFEST["tables"].items():
    print(f"  {CATALOG}.{t:40s} {len(m['files']):4d} Dateien {m['rows']:>12,} Zeilen  (data/{m['data_dir']})")

# COMMAND ----------

# DBTITLE 1,1. Dateien prüfen (SHA-256)
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for ch in iter(lambda: fh.read(1 << 20), b""): h.update(ch)
    return h.hexdigest()

problems = []
for t, m in MANIFEST["tables"].items():
    for e in m["files"]:
        p = f"{DATA_DIR}/{m['data_dir']}/{e['name']}"
        if not os.path.isfile(p): problems.append(f"fehlt: {m['data_dir']}/{e['name']}")
        elif os.path.getsize(p) != e["bytes"] or sha256(p) != e["sha256"]: problems.append(f"abweichend: {m['data_dir']}/{e['name']}")
assert not problems, f"{len(problems)} Dateien fehlen oder weichen ab, z. B. {problems[:5]}"
print("Alle", sum(len(m["files"]) for m in MANIFEST["tables"].values()), "Dateien unverändert")

# COMMAND ----------

# DBTITLE 1,2. Catalog und Schemas anlegen
# Catalog prüfen und bei Bedarf anlegen
CATALOG_EXISTS = spark.sql("SHOW CATALOGS").where(F.col("catalog") == CATALOG).count() > 0
if CATALOG_EXISTS:
    print(f"Catalog {CATALOG} vorhanden")
else:
    print(f"Catalog {CATALOG} nicht gefunden – wird angelegt")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` COMMENT 'BIC Case Study – synthetische Daten'")
    except Exception as e:
        raise RuntimeError(
            f"Catalog {CATALOG} konnte nicht angelegt werden. Benötigt wird das Recht CREATE CATALOG auf dem Metastore; "
            f"je nach Metastore-Konfiguration zusätzlich ein MANAGED LOCATION. Bitte den Catalog von einem Admin anlegen lassen "
            f"und das Notebook erneut ausführen.\nUrsache: {str(e)[:300]}") from e
    print(f"Catalog {CATALOG} angelegt")

# Schemas anlegen; Predictive Optimization würde die Tabellen im Hintergrund optimieren und den Ausgangszustand verändern
PO_STATUS = {}
for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{s}` COMMENT 'BIC Case Study – synthetische Daten'")
    try:
        spark.sql(f"ALTER SCHEMA `{CATALOG}`.`{s}` DISABLE PREDICTIVE OPTIMIZATION")
        PO_STATUS[s] = "DISABLE"
    except Exception as e:
        PO_STATUS[s] = "nicht geändert"
        print(f"Hinweis: Predictive Optimization für {CATALOG}.{s} konnte nicht deaktiviert werden (fehlende Rechte oder Feature nicht verfügbar).\n"
              "Bitte keine Optimierungen vor den entsprechenden Aufgaben ausführen.", str(e)[:200])
print("Predictive Optimization:", PO_STATUS)

# COMMAND ----------

# DBTITLE 1,2a. Catalog warehousing anlegen
# Katalog warehousing prüfen und bei Bedarf anlegen
WAREHOUSING = "warehousing"
if spark.sql("SHOW CATALOGS").filter(F"catalog = '{WAREHOUSING}'").count() > 0:
    print(f"Catalog {WAREHOUSING} vorhanden")
else:
    print(f"Catalog {WAREHOUSING} nicht gefunden – wird angelegt")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{WAREHOUSING}` COMMENT 'BIC Case Study – Ziel-Katalog für Warehousing-Aufgaben'")
    except Exception as e:
        raise RuntimeError(
            f"Catalog {WAREHOUSING} konnte nicht angelegt werden. Benötigt wird das Recht CREATE CATALOG auf dem Metastore; "
            f"je nach Metastore-Konfiguration zusätzlich ein MANAGED LOCATION. Bitte den Catalog von einem Admin anlegen lassen "
            f"und das Notebook erneut ausführen.\nUrsache: {str(e)[:300]}") from e
    print(f"Catalog {WAREHOUSING} angelegt")

# COMMAND ----------

# DBTITLE 1,3. Tabellen anlegen und laden (1 Parquet-Datei = 1 Delta-Datei)
TBLPROPS = {
    "delta.enableVariantShredding": "false",      # Ausgangszustand: VARIANT unshredded
    "delta.autoOptimize.optimizeWrite": "false",  # Dateilayout 1:1 wie im Repo
    "delta.autoOptimize.autoCompact": "false",
}

def load_table(m):
    target, parquet_schema = StructType.fromDDL(m["ddl"]), StructType.fromDDL(m["parquet_ddl"])
    spark.sql(f"DROP TABLE IF EXISTS {fq(m)}")
    props = ", ".join(f"'{k}' = '{v}'" for k, v in TBLPROPS.items())
    spark.sql(f"CREATE TABLE {fq(m)} ({m['ddl']}) COMMENT 'Synthetische Case-Study-Daten {MANIFEST['dataset_version']} "
              f"(keine Produktivdaten)' TBLPROPERTIES ({props})")
    # jede Datei als eigene Partition lesen -> genau eine Ausgabedatei je Eingabedatei, Reihenfolge bleibt erhalten
    parts = [spark.read.schema(parquet_schema).parquet("file:" + f"{DATA_DIR}/{m['data_dir']}/{e['name']}").coalesce(1) for e in m["files"]]
    df = reduce(lambda a, b: a.unionAll(b), parts)
    df = df.select(*[F.parse_json(F.col(f"`{c}`")).alias(c) if c in m["variant_columns"] else F.col(f"`{c}`") for c in target.fieldNames()])
    df.write.mode("append").saveAsTable(fq(m))

for t, m in MANIFEST["tables"].items():
    for attempt in range(10):   # frisch synchronisierte Workspace-Dateien sind für Spark manchmal erst nach kurzer Zeit sichtbar
        try:
            load_table(m); break
        except Exception as e:
            if "PATH_NOT_FOUND" not in str(e) or attempt == 9: raise
            print(f"{t}: Dateien noch nicht sichtbar, neuer Versuch in 30 s"); time.sleep(30)
    print("geladen:", f"{CATALOG}.{t}")

# COMMAND ----------

# DBTITLE 1,4. Kontrolle gegen Manifest
def fingerprint(df):
    """Reihenfolgeunabhängiger Inhalts-Fingerprint: Anzahl, Summe und XOR von xxhash64(to_json(Zeile))."""
    r = (df.select(F.xxhash64(F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns]))).alias("h"))
           .agg(F.count("*").alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s"), F.bit_xor("h").alias("x")).first())
    return dict(rows=int(r["n"]), sum=str(r["s"]), xor=int(r["x"]) if r["x"] is not None else None)

rows = []
for t, m in MANIFEST["tables"].items():
    d = spark.sql(f"DESCRIBE DETAIL {fq(m)}").first()
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {fq(m)}").collect()}
    fp = fingerprint(spark.table(fq(m)))
    unclustered = not d.clusteringColumns and props.get("clusterByAuto", "false") != "true"
    ok = (fp == m["fingerprint"] and d.numFiles == len(m["files"]) and props.get("delta.enableVariantShredding") == "false" and unclustered)
    rows.append((f"{CATALOG}.{t}", fp["rows"], m["rows"], int(d.numFiles), len(m["files"]), round(d.sizeInBytes / 1e6, 1),
                 props.get("delta.enableVariantShredding"), unclustered, fp == m["fingerprint"], ok))
RESULT = spark.createDataFrame(rows, "table STRING, rows BIGINT, rows_expected BIGINT, files INT, files_expected INT, size_mb DOUBLE, "
                                     "variant_shredding STRING, unclustered BOOLEAN, content_identical BOOLEAN, ok BOOLEAN")
display(RESULT)
ALL_OK = RESULT.where("NOT ok").count() == 0
print("Alles korrekt angelegt – weiter mit 02_Case_Study_Aufgaben (auf einem SQL Warehouse)" if ALL_OK else "FEHLER: mindestens eine Tabelle weicht ab (siehe Tabelle oben)")

# COMMAND ----------

# DBTITLE 1,Ergebnis (für automatische Aufrufe)
dbutils.notebook.exit(json.dumps({"status": "OK" if ALL_OK else "FEHLER", "catalog": CATALOG, "catalog_angelegt": not CATALOG_EXISTS,
                                  "predictive_optimization": PO_STATUS, "tables": {r.table: r.ok for r in RESULT.collect()}}))