"""
inspect_schema.py — dry run, no Memgraph needed
================================================
Usage:
    python inspect_schema.py
    TABLE_PREFIXES=apm_ python inspect_schema.py
    TABLE_PREFIXES=apm_,evt_ python inspect_schema.py
"""

import os
import sys
from pathlib import Path

import yaml

from graflo.hq import GraphEngine
from graflo.db.connection.onto import PostgresConfig
from graflo.onto import DBType

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Name normalisation ────────────────────────────────────────────────────────
# GraFlo stores names as tuples: ("pilothouse_admin", "apm_1") → "apm_1"

def to_str(name) -> str:
    if isinstance(name, (tuple, list)):
        return str(name[-1])
    return str(name)


def matches(name, prefixes: list[str]) -> bool:
    n = to_str(name)
    return any(n.startswith(p) for p in prefixes)


# ── Config ────────────────────────────────────────────────────────────────────

def build_postgres_config() -> PostgresConfig:
    missing = [v for v in ["POSTGRES_URI", "POSTGRES_USERNAME",
                            "POSTGRES_PASSWORD", "POSTGRES_DATABASE"]
               if not os.getenv(v)]
    if missing:
        print(f"[error] Missing env vars: {', '.join(missing)}")
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


# ── Filtering ─────────────────────────────────────────────────────────────────

def filter_manifest(manifest, prefixes: list[str]):
    """
    Filter ALL three sections of the manifest:
      1. schema         — vertex_config and edge_config
      2. ingestion_model — resources
      3. bindings        — connectors
    """
    schema          = manifest.require_schema()
    ingestion_model = manifest.require_ingestion_model()

    # ── 1. Schema: vertices ───────────────────────────────────────────────
    orig_vertices = schema.core_schema.vertex_config.vertices
    kept_vertices = [v for v in orig_vertices if matches(v.name, prefixes)]
    kept_names    = {to_str(v.name) for v in kept_vertices}

    # Debug: show what matched and what didn't
    all_names    = [to_str(v.name) for v in orig_vertices]
    dropped      = [n for n in all_names if n not in kept_names]
    print(f"\n  [filter] {len(kept_vertices)}/{len(orig_vertices)} vertex tables kept")
    if kept_names:
        print(f"           kept    : {', '.join(sorted(kept_names))}")
    if dropped:
        print(f"           dropped : {', '.join(sorted(dropped)[:10])}"
              + (" …" if len(dropped) > 10 else ""))

    if not kept_vertices:
        print(f"\n  [warn] No tables match prefixes {prefixes}.")
        print(f"         All table names found: {', '.join(sorted(all_names)[:20])}")
        print(f"         Showing ALL tables so you can see what exists.\n")
        return manifest

    # ── 1. Schema: edges (keep only if BOTH endpoints are in kept_names) ──
    orig_edges  = schema.core_schema.edge_config
    kept_edges  = {
        k: e for k, e in orig_edges.items()
        if matches(k, prefixes)
        or (
            to_str(getattr(e, "source_vertex", "")) in kept_names
            and to_str(getattr(e, "target_vertex", "")) in kept_names
        )
    }
    print(f"           edges kept: {len(kept_edges)}/{len(orig_edges)}")

    new_vertex_cfg = schema.core_schema.vertex_config.model_copy(
        update={"vertices": kept_vertices}
    )
    new_core = schema.core_schema.model_copy(
        update={"vertex_config": new_vertex_cfg, "edge_config": kept_edges}
    )
    new_schema = schema.model_copy(update={"core_schema": new_core})

    # ── 2. Ingestion model: resources ─────────────────────────────────────
    kept_resources = [
        r for r in ingestion_model.resources
        if to_str(r.name) in kept_names or matches(r.name, prefixes)
    ]
    new_ingestion_model = ingestion_model.model_copy(
        update={"resources": kept_resources}
    )

    # ── 3. Bindings: connectors ───────────────────────────────────────────
    bindings = getattr(manifest, "bindings", None)
    new_bindings = bindings
    if bindings is not None:
        orig_connectors = getattr(bindings, "connectors", []) or []
        kept_connectors = [
            c for c in orig_connectors
            if to_str(getattr(c, "table_name", getattr(c, "name", ""))) in kept_names
            or matches(
                getattr(c, "table_name", getattr(c, "name", "")), prefixes
            )
        ]
        new_bindings = bindings.model_copy(
            update={"connectors": kept_connectors}
        )

    update = {
        "schema": new_schema,
        "ingestion_model": new_ingestion_model,
    }
    if new_bindings is not None:
        update["bindings"] = new_bindings

    return manifest.model_copy(update=update)


# ── Display ───────────────────────────────────────────────────────────────────

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

    print(f"\n  VERTEX TYPES  ({len(vertices)} tables → nodes)\n")
    for v in sorted(vertices, key=lambda x: to_str(x.name)):
        fields     = getattr(v, "fields", []) or []
        identities = (getattr(v, "identity_fields", None)
                      or getattr(v, "identities", None) or [])
        field_names = [to_str(f if isinstance(f, str) else getattr(f, "name", f))
                       for f in fields]
        id_names    = [to_str(i if isinstance(i, str) else getattr(i, "name", i))
                       for i in identities]
        print(f"    ● {to_str(v.name)}")
        if id_names:
            print(f"        identity : {', '.join(id_names)}")
        if field_names:
            chunks, line = [], []
            for fn in field_names:
                line.append(fn)
                if len(", ".join(line)) > 50:
                    chunks.append(", ".join(line[:-1]))
                    line = [fn]
            chunks.append(", ".join(line))
            pad = " " * 19
            print(f"        fields   : {chunks[0]}")
            for c in chunks[1:]:
                print(f"{pad}{c}")

    print(f"\n  EDGE TYPES  ({len(edges)} tables → relationships)\n")
    if edges:
        for name, e in sorted(edges.items(), key=lambda x: to_str(x[0])):
            src = to_str(getattr(e, "source_vertex", "?"))
            tgt = to_str(getattr(e, "target_vertex", "?"))
            print(f"    ─▶ {to_str(name)}")
            print(f"        {src}  →  {tgt}")
    else:
        print("    (none — check that FK constraints exist on your tables)")

    print(f"\n  RESOURCES  ({len(resources)} ingestion pipelines)\n")
    for r in sorted(resources, key=lambda x: to_str(x.name)):
        print(f"    ▸ {to_str(r.name)}")

    print("\n" + "═" * W)
    print("  Manifest saved → generated-manifest.yaml")
    print("  When Memgraph is ready:  python ingest.py")
    print("═" * W + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    postgres_conf = build_postgres_config()
    schema_name   = os.getenv("POSTGRES_SCHEMA", "public")
    prefixes      = parse_prefixes()

    print(f"\nConnecting to PostgreSQL — {postgres_conf.database} / {schema_name}")
    if prefixes:
        print(f"Filtering by prefix(es): {', '.join(prefixes)}")

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