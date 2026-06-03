"""
PostgreSQL → Memgraph ingestion via GraFlo
==========================================

Usage:
    # Full ingestion
    python ingest.py

    # Test run — only N rows per table
    INGEST_LIMIT=100 python ingest.py
"""

import logging
import os
from pathlib import Path

import yaml

from graflo.db.connection.onto import PostgresConfig, MemgraphConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Connection configs — all values come from .env / environment variables
# ─────────────────────────────────────────────────────────────────────────────

def build_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        uri=os.environ["POSTGRES_URI"],                     # required
        username=os.environ["POSTGRES_USERNAME"],           # required
        password=os.environ["POSTGRES_PASSWORD"],           # required
        database=os.environ["POSTGRES_DATABASE"],           # required
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
    )


def build_memgraph_config() -> MemgraphConfig:
    return MemgraphConfig(
        uri=os.getenv("MEMGRAPH_URI", "bolt://localhost:7687"),
        username=os.getenv("MEMGRAPH_USERNAME", ""),
        password=os.getenv("MEMGRAPH_PASSWORD", ""),
        database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Row limit helper — for test runs
#
# Set INGEST_LIMIT=100 in your environment to cap every table at 100 rows.
# Leave it unset (or 0) for a full ingestion.
#
# How it works: GraFlo's TableConnector accepts a SelectSpec that lets you
# inject arbitrary SQL clauses. We use it to add "LIMIT N" to every query.
# ─────────────────────────────────────────────────────────────────────────────

def apply_row_limit(engine: GraphEngine, postgres_conf: PostgresConfig,
                    schema_name: str, limit: int):
    """Re-create bindings with a LIMIT clause on every table connector."""
    from graflo.architecture.contract.bindings import Bindings, TableConnector
    from graflo.filter.select import SelectSpec

    bindings = engine.create_bindings(postgres_conf, schema_name=schema_name)

    limited_connectors = []
    for connector in bindings.connectors:
        if isinstance(connector, TableConnector):
            connector = connector.model_copy(
                update={"select_spec": SelectSpec(limit=limit)}
            )
        limited_connectors.append(connector)

    return bindings.model_copy(update={"connectors": limited_connectors})


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run_ingestion(
    postgres_conf: PostgresConfig,
    memgraph_conf: MemgraphConfig,
    schema_name: str = "public",
    row_limit: int | None = None,       # None = all rows
    clear_data: bool = True,
    recreate_schema: bool = True,
    batch_size: int = 1000,
) -> None:

    db_type = memgraph_conf.connection_type
    engine  = GraphEngine(target_db_flavor=db_type)

    # ── Step 1: infer manifest from PostgreSQL information_schema ────────────
    logger.info("Inferring graph manifest from schema '%s'…", schema_name)
    manifest = engine.infer_manifest(postgres_conf, schema_name=schema_name)

    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    logger.info(
        "Found %d vertex type(s) and %d edge type(s).",
        len(schema.core_schema.vertex_config.vertices),
        len(list(schema.core_schema.edge_config.values())),
    )

    # ── Step 2: optionally save manifest YAML for inspection ─────────────────
    manifest_path = Path("generated-manifest.yaml")
    with open(manifest_path, "w") as fh:
        yaml.safe_dump(
            manifest.model_dump(mode="json"),
            fh, default_flow_style=False, sort_keys=False,
        )
    logger.info("Manifest saved → %s  (inspect this to understand what was inferred)", manifest_path)

    # ── Step 3: apply row limit for test runs ─────────────────────────────────
    if row_limit:
        logger.info("TEST MODE: limiting every table to %d rows.", row_limit)
        bindings = apply_row_limit(engine, postgres_conf, schema_name, row_limit)
        manifest = manifest.model_copy(update={"bindings": bindings})

    manifest.finish_init()

    # ── Step 4: ingest into Memgraph ─────────────────────────────────────────
    ingestion_params = IngestionParams(
        clear_data=clear_data,
        batch_size=batch_size,
    )

    logger.info("Ingesting into Memgraph%s…",
                f" (limit {row_limit} rows/table)" if row_limit else "")
    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=memgraph_conf,
        ingestion_params=ingestion_params,
        recreate_schema=recreate_schema,
    )

    print("\n" + "=" * 60)
    print("  PostgreSQL → Memgraph  ✓")
    print("=" * 60)
    print(f"  Schema      : {schema.metadata.name}")
    print(f"  Vertex types: {len(schema.core_schema.vertex_config.vertices)}")
    print(f"  Edge types  : {len(list(schema.core_schema.edge_config.values()))}")
    print(f"  Resources   : {len(ingestion_model.resources)}")
    if row_limit:
        print(f"  Row limit   : {row_limit} per table  ← test mode")
    print("=" * 60)
    print("  Explore → http://localhost:3000  (Memgraph Lab)")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    # Load .env file if python-dotenv is installed (optional convenience)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    postgres_conf = build_postgres_config()
    memgraph_conf = build_memgraph_config()

    # INGEST_LIMIT=100 → test mode (100 rows per table)
    # INGEST_LIMIT unset or 0 → full ingestion
    row_limit_env = int(os.getenv("INGEST_LIMIT", "0"))

    run_ingestion(
        postgres_conf=postgres_conf,
        memgraph_conf=memgraph_conf,
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
        row_limit=row_limit_env if row_limit_env > 0 else None,
        clear_data=True,
        recreate_schema=True,
        batch_size=1000,
    )