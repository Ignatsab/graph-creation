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


# ── Name normalisation ────────────────────────────────────────────────────────
# GraFlo stores vertex/edge names as tuples e.g. ("pilothouse_admin", "apm_1")
# rather than plain strings. to_str() extracts just the table name part.

def to_str(name) -> str:
    if isinstance(name, (tuple, list)):
        return str(name[-1])   # last element = table name, ignore schema prefix
    return str(name)


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
    raw = os.getenv("TABLE_PREFIXES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()]


def matches(name, prefixes: list[str]) -> bool:
    """Check if a vertex/edge name (string or tuple) starts with any prefix."""
    n = to_str(name)
    return any(n.startswith(p) for p in prefixes)


def filter_manifest(manifest, prefixes: list[str]):
    schema = manifest.require_schema()

    # Filter vertex types
    orig_vertices = schema.core_schema.vertex_config.vertices
    kept_vertices = [v for v in orig_vertices if matches(v.name, prefixes)]
    kept_names    = {to_str(v.name) for v in kept_vertices}

    if not kept_vertices:
        print(f"\n[warn] No tables match prefixes {prefixes}.")
        print("       Check TABLE_PREFIXES in your .env or the schema name.")
        print("       Showing ALL tables instead so you can see what exists.\n")
        return manifest

    # Filter edges — keep if both endpoints are in kept_names,
    # or if the edge table name itself matches a prefix
    orig_edges = schema.core_schema.edge_config
    kept_edges = {
        k: e for k, e in orig_edges.items()
        if matches(k, prefixes)
        or (
            to_str(getattr(e, "source_vertex", "")) in kept_names
            and to_str(getattr(e, "target_vertex", "")) in kept_names
        )
    }

    new_vertex_cfg = schema.core_schema.vertex_config.model_copy(
        update={"vertices": kept_vertices}
    )
    new_core = schema.core_schema.model_copy(
        update={"vertex_config": new_vertex_cfg, "edge_config": kept_edges}
    )
    new_schema  = schema.model_copy(update={"core_schema": new_core})
    return manifest.model_copy(update={"schema": new_schema})


def print_summary(manifest, prefixes: list[str]) -> None:
    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()

    vertices  = schema.core_schema.vertex_config.vertices
    edges     = dict(schema.core_schema.edge_config)
    resources = ingestion_model.resources

    W = 62
    print("\n" + "═" * W)
    print(f"  GraFlo schema inference — {schema.metadata.name}")
    if prefixes:
        print(f"  Prefix filter : {', '.join(prefixes)}")
    print("═" * W)

    # ── Vertices ──────────────────────────────────────────────────────────
    print(f"\n  VERTEX TYPES  ({len(vertices)} tables → nodes)\n")
    for v in sorted(vertices, key=lambda x: to_str(x.name)):
        fields     = getattr(v, "fields", []) or []
        identities = (getattr(v, "identity_fields", None)
                      or getattr(v, "identities", None) or [])
        field_names = [
            to_str(f if isinstance(f, str) else getattr(f, "name", f))
            for f in fields
        ]
        id_names = [
            to_str(i if isinstance(i, str) else getattr(i, "name", i))
            for i in identities
        ]
        print(f"    ● {to_str(v.name)}")
        if id_names:
            print(f"        identity : {', '.join(id_names)}")
        if field_names:
            # wrap long field lists
            line, lines = [], []
            for fn in field_names:
                line.append(fn)
                if len(", ".join(line)) > 50:
                    lines.append(", ".join(line[:-1]))
                    line = [fn]
            lines.append(", ".join(line))
            pad = " " * 19
            print(f"        fields   : {lines[0]}")
            for l in lines[1:]:
                print(f"{pad}{l}")

    # ── Edges ─────────────────────────────────────────────────────────────
    print(f"\n  EDGE TYPES  ({len(edges)} tables → relationships)\n")
    if edges:
        for name, e in sorted(edges.items(), key=lambda x: to_str(x[0])):
            src = to_str(getattr(e, "source_vertex", "?"))
            tgt = to_str(getattr(e, "target_vertex", "?"))
            print(f"    ─▶ {to_str(name)}")
            print(f"        {src}  →  {tgt}")
    else:
        print("    (none — GraFlo detects edges from FK constraints;")
        print("     if your apm_ tables have no FK constraints, this is expected)")

    # ── Resources ─────────────────────────────────────────────────────────
    print(f"\n  RESOURCES  ({len(resources)} ingestion pipelines)\n")
    for r in sorted(resources, key=lambda x: to_str(x.name)):
        print(f"    ▸ {to_str(r.name)}")

    print("\n" + "═" * W)
    print("  Manifest saved → generated-manifest.yaml")
    print("  When Memgraph is ready, run:  python ingest.py")
    print("═" * W + "\n")


def main() -> None:
    postgres_conf = build_postgres_config()
    schema_name   = os.getenv("POSTGRES_SCHEMA", "public")
    prefixes      = parse_prefixes()

    print(f"\nConnecting to PostgreSQL — {postgres_conf.database} / {schema_name}")
    if prefixes:
        print(f"Filtering tables by prefix(es): {', '.join(prefixes)}")

    engine   = GraphEngine(target_db_flavor=DBType.MEMGRAPH)
    manifest = engine.infer_manifest(postgres_conf, schema_name=schema_name)

    if prefixes:
        manifest = filter_manifest(manifest, prefixes)

    out = Path("generated-manifest.yaml")
    with open(out, "w") as fh:
        yaml.safe_dump(
            manifest.model_dump(mode="json"),
            fh, default_flow_style=False, sort_keys=False,
        )

    print_summary(manifest, prefixes)


if __name__ == "__main__":
    main()