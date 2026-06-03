"""
ingest_from_yaml.py — advanced variant
=======================================
Use this when:
  • You have already run ingest.py once and saved generated-manifest.yaml
  • You want to tweak the manifest (add transforms, rename fields, etc.)
    before re-running without hitting PostgreSQL's information_schema again.
  • You want to add custom edges or vertex filters.

Run:
    python ingest_from_yaml.py
"""

import logging
import os
from pathlib import Path

import yaml

from graflo.architecture.contract.manifest import GraphManifest
from graflo.db.connection.onto import PostgresConfig, MemgraphConfig
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams
from graflo.hq.connection_provider import (
    InMemoryConnectionProvider,
    PostgresGeneralizedConnConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


MANIFEST_PATH = Path("generated-manifest.yaml")


def load_manifest(path: Path) -> GraphManifest:
    with open(path) as fh:
        data = yaml.safe_load(fh)
    return GraphManifest.model_validate(data)


def build_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        uri=os.getenv("POSTGRES_URI", "postgresql://localhost:5432"),
        username=os.getenv("POSTGRES_USERNAME", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("POSTGRES_DATABASE", "mydb"),
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
    )


def build_memgraph_config() -> MemgraphConfig:
    return MemgraphConfig(
        uri=os.getenv("MEMGRAPH_URI", "bolt://localhost:7687"),
        username=os.getenv("MEMGRAPH_USERNAME", ""),
        password=os.getenv("MEMGRAPH_PASSWORD", ""),
        database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
    )


if __name__ == "__main__":
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"{MANIFEST_PATH} not found. Run ingest.py first to generate it."
        )

    postgres_conf = build_postgres_config()
    memgraph_conf = build_memgraph_config()

    # ── Load manifest from YAML ────────────────────────────────────────────
    logger.info("Loading manifest from %s", MANIFEST_PATH)
    manifest = load_manifest(MANIFEST_PATH)

    # ── (Optional) tweak the manifest here ────────────────────────────────
    # Example: add a filter to only ingest active users
    # schema = manifest.require_schema()
    # for v in schema.core_schema.vertex_config.vertices:
    #     if v.name == "users":
    #         v.filters = [{"field": "active", "value": True}]

    # ── Re-create bindings (always needs live PostgreSQL connection) ────────
    engine = GraphEngine(target_db_flavor=memgraph_conf.connection_type)
    bindings = engine.create_bindings(postgres_conf, schema_name="public")

    # ── Wire connection provider (keeps secrets out of YAML) ───────────────
    provider = InMemoryConnectionProvider()
    provider.register_generalized_config(
        conn_proxy="postgres_source",
        config=PostgresGeneralizedConnConfig(config=postgres_conf),
    )
    provider.bind_from_bindings(bindings=bindings)

    manifest = manifest.model_copy(update={"bindings": bindings})
    manifest.finish_init()

    # ── Ingest ─────────────────────────────────────────────────────────────
    ingestion_params = IngestionParams(
        clear_data=True,
        batch_size=int(os.getenv("BATCH_SIZE", "1000")),
    )

    engine.define_and_ingest(
        manifest=manifest,
        target_db_config=memgraph_conf,
        ingestion_params=ingestion_params,
        recreate_schema=True,
        connection_provider=provider,
    )
    logger.info("✓ Done — data is in Memgraph.")
