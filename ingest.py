"""
PostgreSQL → Memgraph ingestion via GraFlo
==========================================
Run:
    python ingest.py
or with custom env file:
    GRAFLO_ENV=.env.prod python ingest.py
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
# 1.  Connection configs
#     Prefer environment variables; fall back to hard-coded defaults for local dev.
# ─────────────────────────────────────────────────────────────────────────────

def build_postgres_config() -> PostgresConfig:
    """Build PostgresConfig from environment variables."""
    return PostgresConfig(
        uri=os.getenv("POSTGRES_URI", "postgresql://localhost:5432"),
        username=os.getenv("POSTGRES_USERNAME", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("POSTGRES_DATABASE", "mydb"),
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
    )


def build_memgraph_config() -> MemgraphConfig:
    """Build MemgraphConfig from environment variables."""
    return MemgraphConfig(
        uri=os.getenv("MEMGRAPH_URI", "bolt://localhost:7687"),
        username=os.getenv("MEMGRAPH_USERNAME", ""),
        password=os.getenv("MEMGRAPH_PASSWORD", ""),
        database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Schema inference
#     GraFlo reads PostgreSQL's information_schema to automatically detect:
#       • Vertex tables  → tables with a primary key + descriptive columns
#       • Edge tables    → tables with 2+ foreign keys (junction / relation tables)
#     It then creates a full GraphManifest (schema + ingestion_model + bindings).
# ─────────────────────────────────────────────────────────────────────────────

def infer_and_save_manifest(
    postgres_conf: PostgresConfig,
    memgraph_conf: MemgraphConfig,
    schema_name: str = "public",
    manifest_output_path: Path = Path("generated-manifest.yaml"),
) -> tuple:
    """
    Returns (manifest, engine).
    Saves the inferred manifest YAML to *manifest_output_path* for inspection /
    version-control.
    """
    db_type = memgraph_conf.connection_type          # DBType.MEMGRAPH
    engine  = GraphEngine(target_db_flavor=db_type)

    logger.info("Inferring graph manifest from PostgreSQL schema '%s'…", schema_name)
    manifest = engine.infer_manifest(postgres_conf, schema_name=schema_name)

    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()

    logger.info(
        "Inferred %d vertex type(s) and %d edge type(s).",
        len(schema.core_schema.vertex_config.vertices),
        len(list(schema.core_schema.edge_config.values())),
    )

    # ── Optional: save YAML for review / manual tweaks ──────────────────────
    manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_output_path, "w") as fh:
        yaml.safe_dump(
            manifest.model_dump(mode="json"),
            fh,
            default_flow_style=False,
            sort_keys=False,
        )
    logger.info("Manifest saved to %s", manifest_output_path)

    return manifest, engine


# ─────────────────────────────────────────────────────────────────────────────
# 3.  (Optional) Override / extend the inferred manifest
#     Useful when you want date-range filtering or need to adjust field names.
# ─────────────────────────────────────────────────────────────────────────────

def override_bindings_with_time_filter(
    engine: GraphEngine,
    postgres_conf: PostgresConfig,
    schema_name: str = "public",
    datetime_columns: dict | None = None,
) :
    """
    Re-create bindings with per-table datetime columns for time-range filtering.

    Example datetime_columns:
        {
            "orders":   "created_at",
            "products": "updated_at",
        }
    """
    if datetime_columns is None:
        return None   # caller uses inferred bindings from manifest

    return engine.create_bindings(
        postgres_conf,
        schema_name=schema_name,
        datetime_columns=datetime_columns,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Ingestion
# ─────────────────────────────────────────────────────────────────────────────

def run_ingestion(
    postgres_conf: PostgresConfig,
    memgraph_conf: MemgraphConfig,
    schema_name: str = "public",
    # ── Time-range filtering (optional) ──────────────────────────────────────
    datetime_columns: dict | None = None,
    datetime_after:  str | None = None,    # ISO, e.g. "2024-01-01"
    datetime_before: str | None = None,    # ISO, e.g. "2025-01-01"
    # ── Ingestion behaviour ──────────────────────────────────────────────────
    clear_data:       bool = True,
    recreate_schema:  bool = True,
    batch_size:       int  = 1000,
) -> None:
    manifest_path = Path("generated-manifest.yaml")

    # ── Step 1: infer (or reload) manifest ────────────────────────────────
    manifest, engine = infer_and_save_manifest(
        postgres_conf, memgraph_conf,
        schema_name=schema_name,
        manifest_output_path=manifest_path,
    )

    # ── Step 2: (optional) attach time-filter-aware bindings ───────────────
    custom_bindings = override_bindings_with_time_filter(
        engine, postgres_conf, schema_name, datetime_columns
    )
    if custom_bindings is not None:
        manifest = manifest.model_copy(update={"bindings": custom_bindings})

    manifest.finish_init()

    # ── Step 3: ingestion params ────────────────────────────────────────────
    ingestion_params = IngestionParams(
        clear_data=clear_data,
        batch_size=batch_size,
        **({"datetime_after":  datetime_after}  if datetime_after  else {}),
        **({"datetime_before": datetime_before} if datetime_before else {}),
    )

    # ── Step 4: define schema in Memgraph + stream data ─────────────────────
    logger.info("Starting ingestion into Memgraph…")
    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=memgraph_conf,
        ingestion_params=ingestion_params,
        recreate_schema=recreate_schema,
    )
    logger.info("✓ Ingestion complete.")

    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()
    print("\n" + "=" * 60)
    print("  PostgreSQL → Memgraph  ✓")
    print("=" * 60)
    print(f"  Schema      : {schema.metadata.name}")
    print(f"  Vertex types: {len(schema.core_schema.vertex_config.vertices)}")
    print(f"  Edge types  : {len(list(schema.core_schema.edge_config.values()))}")
    print(f"  Resources   : {len(ingestion_model.resources)}")
    print("=" * 60)
    print("  Explore in Memgraph Lab → http://localhost:3000")
    print("  or via bolt://localhost:7687")
    print("=" * 60 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    postgres_conf = build_postgres_config()
    memgraph_conf = build_memgraph_config()

    run_ingestion(
        postgres_conf=postgres_conf,
        memgraph_conf=memgraph_conf,
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
        # ── Uncomment to filter by date ───────────────────────────────────
        # datetime_columns={"orders": "created_at", "users": "created_at"},
        # datetime_after="2024-01-01",
        # datetime_before="2025-01-01",
        clear_data=True,
        recreate_schema=True,
        batch_size=1000,
    )
