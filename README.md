# PostgreSQL → Memgraph via GraFlo

A ready-to-run implementation for migrating relational data from PostgreSQL into the [Memgraph](https://memgraph.com) knowledge graph using [GraFlo](https://growgraph.github.io/graflo/).

---

## The Plan

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Migration Pipeline                            │
│                                                                      │
│  PostgreSQL (3NF)                              Memgraph (LPG)        │
│  ─────────────                                 ──────────────        │
│  Tables with PK         ───► GraFlo ───►       Vertices              │
│  Junction tables (2 FK)                        Edges                 │
│  Column types (PG)                             Typed properties      │
│  FK relationships                              Edge source/target    │
└──────────────────────────────────────────────────────────────────────┘
```

### Step-by-step

**Step 1 — Prerequisites**
Ensure your PostgreSQL schema follows 3NF conventions:
- Every entity table has a `PRIMARY KEY`.
- Every relationship table has 2+ `FOREIGN KEY` columns pointing to entity tables.
- Column types use standard PG types (INT, VARCHAR, TIMESTAMP, DECIMAL, BOOL).

**Step 2 — Spin up services**
```bash
cp .env.example .env       # edit with your credentials
docker compose up -d       # starts PostgreSQL + Memgraph + Memgraph Lab
```

**Step 3 — Install GraFlo**
```bash
pip install -r requirements.txt
# or: pip install graflo python-dotenv pyyaml
```

**Step 4 — Infer schema & ingest**
```bash
# Auto-infer + ingest in one shot
python ingest.py
```
GraFlo will:
1. Connect to PostgreSQL and read `information_schema`.
2. Classify tables as vertex types (entities) or edge types (relationships).
3. Map PostgreSQL column types → GraFlo field types (`INT`, `STRING`, `FLOAT`, `DATETIME`, `BOOL`).
4. Build a `GraphManifest` (schema + ingestion model + bindings) and save it to `generated-manifest.yaml`.
5. Create the schema in Memgraph (indexes, constraints).
6. Stream rows from each PG table and upsert them into Memgraph via Bolt.

**Step 5 — Explore in Memgraph Lab**
Open [http://localhost:3000](http://localhost:3000) and run Cypher queries:
```cypher
MATCH (n) RETURN n LIMIT 50;
MATCH (u:users)-[r]->(p:products) RETURN u, r, p LIMIT 25;
```

---

## File Structure

```
.
├── ingest.py               # Main script — auto-infer + ingest (run this first)
├── ingest_from_yaml.py     # Advanced — reload saved manifest + manual tweaks
├── init_db.sql             # Sample PG schema with seed data (mounted by docker-compose)
├── docker-compose.yml      # PostgreSQL + Memgraph + Memgraph Lab
├── .env.example            # Environment variable template
├── requirements.txt
└── generated-manifest.yaml # Created on first run — inspect & version-control this
```

---

## How GraFlo Classifies Your Tables

| Table shape                                   | Classified as | Becomes in Memgraph |
|-----------------------------------------------|---------------|----------------------|
| Has PK + descriptive columns (no/few FKs)     | **Vertex**    | Node with properties |
| Has 2+ FKs (optionally + extra columns)       | **Edge**      | Relationship         |
| Has 2+ FKs + extra payload columns            | **Edge**      | Relationship with properties |
| Self-referential (FK → same table, e.g. `follows`) | **Edge** | Self-loop relationship |

---

## Type Mapping

| PostgreSQL type                        | GraFlo / Memgraph type |
|----------------------------------------|------------------------|
| `INTEGER`, `BIGINT`, `SERIAL`          | `INT`                  |
| `VARCHAR`, `TEXT`, `CHAR`              | `STRING`               |
| `DECIMAL`, `NUMERIC`, `REAL`, `FLOAT8` | `FLOAT`                |
| `TIMESTAMP`, `DATE`, `TIME`            | `DATETIME`             |
| `BOOLEAN`                              | `BOOL`                 |

---

## Customisation

### Date-range filtering
Only ingest rows within a time window:
```python
run_ingestion(
    postgres_conf=postgres_conf,
    memgraph_conf=memgraph_conf,
    datetime_columns={"orders": "created_at", "users": "registered_at"},
    datetime_after="2024-01-01",
    datetime_before="2025-01-01",
)
```

### Tweaking the inferred manifest
Run `ingest.py` once to generate `generated-manifest.yaml`, edit it, then use `ingest_from_yaml.py` to replay with your changes.

### Multiple PostgreSQL schemas
Call `run_ingestion(schema_name="sales")` and `run_ingestion(schema_name="inventory")` in sequence — each call creates its own sub-graph in Memgraph.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Table classified as vertex instead of edge | Add proper FK constraints to the table |
| Fields missing from nodes | Ensure columns have non-null names; check for reserved-word collisions (GraFlo sanitizes these) |
| `MemgraphConfig` not found | Upgrade graflo: `pip install -U graflo` |
| Connection refused (Memgraph) | Check `docker compose ps`; bolt port 7687 must be open |
| Schema inference returns 0 tables | Confirm `POSTGRES_SCHEMA` env var matches the actual PG schema name |

---

## Requirements

- Python 3.11+
- Docker + Docker Compose (for local dev)
- OR: existing PostgreSQL and Memgraph instances with network access
