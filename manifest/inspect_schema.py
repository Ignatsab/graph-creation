"""
inspect_schema.py — dry run, no Memgraph needed
================================================
Connects to PostgreSQL, runs GraFlo's schema inference, prints a full
summary of what was found, and saves the manifest to YAML.

Nothing is written to any graph database.

Usage:
    python inspect_schema.py

    # Filter to specific table prefixes
    TABLE_PREFIXES=apm_ python inspect_schema.py

    # Multiple prefixes
    TABLE_PREFIXES=apm_,evt_,ref_ python inspect_schema.py
"""

import os
import sys
from pathlib import Path

import yaml

from graflo.hq import GraphEngine
from graflo.db.connection.onto import PostgresConfig
from graflo.onto import DBType

# ── Load .env if python-dotenv is available ───────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def build_postgres_config() -> PostgresConfig:
    missing = [v for v in ["POSTGRES_URI", "POSTGRES_USERNAME",
                            "POSTGRES_PASSWORD", "POSTGRES_DATABASE"]
               if not os.getenv(v)]
    if missing:
        print(f"[error] Missing environment variables: {', '.join(missing)}")
        print("        Copy .env.example → .env and fill in your credentials.")
        sys.exit(1)

    return PostgresConfig(
        uri=os.environ["POSTGRES_URI"],
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DATABASE"],
        schema_name=os.getenv("POSTGRES_SCHEMA", "public"),
    )


def parse_prefixes() -> list[str]:
    """Read TABLE_PREFIXES=apm_,evt_ from env → ['apm_', 'evt_']"""
    raw = os.getenv("TABLE_PREFIXES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()]


def filter_manifest(manifest, prefixes: list[str]):
    """
    Keep only vertices (and edges connecting them) whose table names
    start with one of the given prefixes.
    """
    schema = manifest.require_schema()

    def matches(name: str) -> bool:
        return any(name.startswith(p) for p in prefixes)

    # Filter vertex types
    orig_vertices = schema.core_schema.vertex_config.vertices
    kept_vertices = [v for v in orig_vertices if matches(v.name)]
    kept_names    = {v.name for v in kept_vertices}

    if not kept_vertices:
        print(f"\n[warn] No tables match prefixes {prefixes}.")
        print("       Check TABLE_PREFIXES in your .env or the schema name.")
        return manifest   # return unfiltered so the user can see what exists

    # Filter edge types — keep edges where both endpoints are in kept_names
    # OR where the edge table itself matches a prefix
    orig_edges  = schema.core_schema.edge_config
    kept_edges  = {
        k: e for k, e in orig_edges.items()
        if matches(k)
        or (
            getattr(e, "source_vertex", None) in kept_names
            and getattr(e, "target_vertex", None) in kept_names
        )
    }

    # Patch schema in-place (Pydantic v2 model_copy)
    new_vertex_cfg = schema.core_schema.vertex_config.model_copy(
        update={"vertices": kept_vertices}
    )
    new_core = schema.core_schema.model_copy(
        update={"vertex_config": new_vertex_cfg, "edge_config": kept_edges}
    )
    new_schema = schema.model_copy(update={"core_schema": new_core})
    return manifest.model_copy(update={"schema": new_schema})


def print_summary(manifest, prefixes: list[str]) -> None:
    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()

    vertices = schema.core_schema.vertex_config.vertices
    edges    = dict(schema.core_schema.edge_config)
    resources = ingestion_model.resources

    W = 62
    print("\n" + "═" * W)
    print(f"  GraFlo schema inference — {schema.metadata.name}")
    if prefixes:
        print(f"  Prefix filter : {', '.join(prefixes)}")
    print("═" * W)

    # ── Vertices ──────────────────────────────────────────────────────────
    print(f"\n  VERTEX TYPES  ({len(vertices)} tables → nodes)\n")
    for v in sorted(vertices, key=lambda x: x.name):
        fields     = getattr(v, "fields", []) or []
        identities = getattr(v, "identity_fields", []) \
                     or getattr(v, "identities", []) or []
        field_names = [
            (f if isinstance(f, str) else getattr(f, "name", str(f)))
            for f in fields
        ]
        id_names = [
            (i if isinstance(i, str) else getattr(i, "name", str(i)))
            for i in identities
        ]
        print(f"    ● {v.name}")
        if id_names:
            print(f"        identity : {', '.join(id_names)}")
        if field_names:
            print(f"        fields   : {', '.join(field_names)}")

    # ── Edges ─────────────────────────────────────────────────────────────
    print(f"\n  EDGE TYPES  ({len(edges)} tables → relationships)\n")
    if edges:
        for name, e in sorted(edges.items()):
            src = getattr(e, "source_vertex", "?")
            tgt = getattr(e, "target_vertex", "?")
            w   = getattr(e, "weight_field",  None)
            print(f"    ─▶ {name}")
            print(f"        {src}  →  {tgt}")
            if w:
                print(f"        weight : {w}")
    else:
        print("    (none detected — check that FK constraints exist on your tables)")

    # ── Resources ─────────────────────────────────────────────────────────
    print(f"\n  RESOURCES  ({len(resources)} ingestion pipelines)\n")
    for r in sorted(resources, key=lambda x: x.name):
        print(f"    ▸ {r.name}")

    print("\n" + "═" * W)
    print("  Manifest saved → generated-manifest.yaml")
    print("  Review it, then run:  python ingest.py")
    print("═" * W + "\n")


def main() -> None:
    postgres_conf = build_postgres_config()
    schema_name   = os.getenv("POSTGRES_SCHEMA", "public")
    prefixes      = parse_prefixes()

    print(f"\nConnecting to PostgreSQL — {postgres_conf.database} / {schema_name}")
    if prefixes:
        print(f"Filtering tables by prefix(es): {', '.join(prefixes)}")

    # Inference only touches PostgreSQL — DBType here only shapes how the
    # manifest schema types are expressed; no graph DB connection is made.
    engine   = GraphEngine(target_db_flavor=DBType.MEMGRAPH)
    manifest = engine.infer_manifest(postgres_conf, schema_name=schema_name)

    if prefixes:
        manifest = filter_manifest(manifest, prefixes)

    # Save YAML
    out = Path("generated-manifest.yaml")
    with open(out, "w") as fh:
        yaml.safe_dump(
            manifest.model_dump(mode="json"),
            fh, default_flow_style=False, sort_keys=False,
        )

    print_summary(manifest, prefixes)


if __name__ == "__main__":
    main()
