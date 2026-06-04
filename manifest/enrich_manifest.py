"""
enrich_manifest.py — LLM-powered manifest enrichment
=====================================================
Loads generated-manifest.yaml + queries PostgreSQL for column metadata,
then asks your local LLM to detect missing relationships, wrong field
types, misclassified tables, and implicit FK patterns.

Saves:
  llm-suggestions.json      — raw LLM output, review before applying
  enriched-manifest.yaml    — manifest with suggestions applied

Usage:
    python enrich_manifest.py                        # apply all suggestions
    python enrich_manifest.py --dry-run              # print suggestions, don't apply
    TABLE_PREFIXES=apm_ python enrich_manifest.py
"""

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

def parse_prefixes() -> list[str]:
    raw = os.getenv("TABLE_PREFIXES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()]


def to_str(name) -> str:
    if isinstance(name, (tuple, list)):
        return str(name[-1])
    return str(name)


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — load manifest
# ─────────────────────────────────────────────────────────────────────────────

def load_manifest(path: Path = Path("generated-manifest.yaml")) -> dict:
    if not path.exists():
        print(f"[error] {path} not found — run inspect_schema.py first.")
        sys.exit(1)
    with open(path) as fh:
        return yaml.safe_load(fh)


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — query PostgreSQL for richer column metadata
# ─────────────────────────────────────────────────────────────────────────────

PG_COLUMNS_SQL = """
SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default,
    CASE WHEN pk.column_name IS NOT NULL THEN 'PK' ELSE '' END AS pk_flag,
    CASE WHEN fk.column_name IS NOT NULL THEN 'FK→' || fk.foreign_table ELSE '' END AS fk_flag
FROM information_schema.columns c

LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema   = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %(schema)s
) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name

LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name, ccu.table_name AS foreign_table
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema   = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        AND tc.table_schema   = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = %(schema)s
) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name

WHERE c.table_schema = %(schema)s
ORDER BY c.table_name, c.ordinal_position
"""

def fetch_column_metadata(prefixes: list[str]) -> dict[str, list[dict]]:
    """Returns {table_name: [{column, type, pk, fk, nullable}, ...]}"""
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_URI"].split("//")[-1].split(":")[0],
        port=int(os.environ["POSTGRES_URI"].split(":")[-1]) if ":" in os.environ["POSTGRES_URI"].split("//")[-1] else 5432,
        user=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DATABASE"],
    )
    schema = os.getenv("POSTGRES_SCHEMA", "public")

    with conn.cursor() as cur:
        cur.execute(PG_COLUMNS_SQL, {"schema": schema})
        rows = cur.fetchall()
    conn.close()

    tables: dict[str, list[dict]] = {}
    for table_name, col, dtype, nullable, default, pk_flag, fk_flag in rows:
        # apply prefix filter
        if prefixes and not any(table_name.startswith(p) for p in prefixes):
            continue
        tables.setdefault(table_name, []).append({
            "column":   col,
            "type":     dtype,
            "nullable": nullable == "YES",
            "pk":       pk_flag == "PK",
            "fk":       fk_flag or None,
        })
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — build compact schema summary for the LLM
# ─────────────────────────────────────────────────────────────────────────────

def build_schema_summary(manifest: dict, col_meta: dict[str, list[dict]]) -> str:
    """
    Produces a compact human-readable summary the LLM can reason about.
    Avoids dumping raw YAML (too verbose, confuses most models).
    """
    lines = []

    # ── current vertices ────────────────────────────────────────────────────
    vertices = (manifest.get("schema", {})
                        .get("core_schema", {})
                        .get("vertex_config", {})
                        .get("vertices", []))
    vertex_names = set()
    lines.append("## CURRENT VERTICES (tables → graph nodes)")
    for v in vertices:
        name = to_str(v.get("name", ""))
        vertex_names.add(name)
        fields = v.get("fields", []) or []
        ids    = v.get("identity_fields", []) or []
        id_cols = [to_str(i.get("name", i) if isinstance(i, dict) else i) for i in ids]
        f_cols  = [
            f"{to_str(f.get('name', f)) if isinstance(f, dict) else to_str(f)}"
            f"({f.get('field_type','?') if isinstance(f, dict) else '?'})"
            for f in fields
        ]
        lines.append(f"  {name}:")
        if id_cols:
            lines.append(f"    identity : {', '.join(id_cols)}")
        if f_cols:
            lines.append(f"    fields   : {', '.join(f_cols)}")

    # ── current edges ────────────────────────────────────────────────────────
    edges = (manifest.get("schema", {})
                     .get("core_schema", {})
                     .get("edge_config", {}) or {})
    lines.append(f"\n## CURRENT EDGES (detected from FK constraints) — {len(edges)} found")
    if edges:
        for ename, e in edges.items():
            src = to_str(e.get("source_vertex", "?"))
            tgt = to_str(e.get("target_vertex", "?"))
            lines.append(f"  {to_str(ename)}: {src} → {tgt}")
    else:
        lines.append("  (none — no FK constraints were found between these tables)")

    # ── PostgreSQL column metadata: highlight implicit FK patterns ──────────
    lines.append("\n## COLUMN DETAIL (from PostgreSQL information_schema)")
    lines.append("## Focus: columns ending in _id/_ref/_fk that have no FK constraint")
    lines.append("## — these are the most likely missing relationships\n")

    id_pattern_cols = []
    for table, cols in sorted(col_meta.items()):
        col_descs = []
        for c in cols:
            flags = []
            if c["pk"]:  flags.append("PK")
            if c["fk"]:  flags.append(c["fk"])
            if c["nullable"]: flags.append("nullable")
            flag_str = f"  [{', '.join(flags)}]" if flags else ""

            # flag columns that look like implicit FKs
            cname = c["column"]
            is_implicit_fk = (
                not c["fk"] and not c["pk"]
                and (cname.endswith("_id") or cname.endswith("_fk")
                     or cname.endswith("_ref") or cname.endswith("_key"))
            )
            marker = " ← POSSIBLE IMPLICIT FK" if is_implicit_fk else ""
            col_descs.append(f"    {cname} ({c['type']}){flag_str}{marker}")
            if is_implicit_fk:
                id_pattern_cols.append((table, cname))

        lines.append(f"  {table}:")
        lines.extend(col_descs)

    if id_pattern_cols:
        lines.append("\n## IMPLICIT FK SUMMARY (columns that look like foreign keys but have no constraint):")
        for table, col in id_pattern_cols:
            # guess the target table from the column name
            guess = col.removesuffix("_id").removesuffix("_fk").removesuffix("_ref")
            lines.append(f"  {table}.{col}  →  possibly references '{guess}' table")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — call the LLM
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a database-to-knowledge-graph schema expert.
You will receive a summary of a PostgreSQL schema and a GraFlo manifest (which maps relational tables to a labeled property graph).
Your job is to detect gaps and suggest improvements so the resulting knowledge graph is well-connected and accurate.

Respond ONLY with a valid JSON object — no explanation, no markdown fences, no preamble.

Use exactly this structure:
{
  "analysis": "2-3 sentence overall assessment",
  "suggested_edges": [
    {
      "name": "edge_label_in_snake_case",
      "source_table": "table_a",
      "source_column": "the_column_that_implies_the_relationship",
      "target_table": "table_b",
      "rationale": "why this relationship likely exists"
    }
  ],
  "field_type_corrections": [
    {
      "table": "table_name",
      "column": "column_name",
      "current_type": "STRING",
      "suggested_type": "DATETIME",
      "rationale": "this column stores ISO timestamps"
    }
  ],
  "reclassify_as_edge": [
    {
      "table": "table_name",
      "source_table": "inferred_source",
      "target_table": "inferred_target",
      "rationale": "this table is a junction/association table"
    }
  ],
  "warnings": [
    "any important caveats about the schema or suggestions"
  ]
}

Rules:
- Only suggest edges between tables that are present in the manifest.
- For suggested_edges, source_column must exist in source_table.
- field_type values must be one of: INT, STRING, FLOAT, DATETIME, BOOL.
- If you are uncertain about something, add it to warnings instead.
- Keep rationale concise (one sentence).
"""

def call_llm(schema_summary: str) -> dict:
    import urllib.request

    base_url = os.environ.get("LLM_API_URL", "").rstrip("/")
    api_key  = os.environ.get("LLM_API_KEY", "")
    model    = os.environ.get("LLM_MODEL", "gpt-4o")

    if not base_url:
        print("[error] LLM_API_URL is not set in .env")
        sys.exit(1)

    payload = json.dumps({
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": schema_summary},
        ],
    }).encode()

    req = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=payload,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    print(f"  Calling {base_url} (model: {model}) …")
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())

    raw_text = body["choices"][0]["message"]["content"].strip()

    # strip accidental markdown fences if model ignores the instruction
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[-1]
        raw_text = raw_text.rsplit("```", 1)[0]

    return json.loads(raw_text)


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — print suggestions in a readable way
# ─────────────────────────────────────────────────────────────────────────────

def print_suggestions(suggestions: dict) -> None:
    W = 64
    print("\n" + "═" * W)
    print("  LLM manifest analysis")
    print("═" * W)

    if suggestions.get("analysis"):
        print(f"\n  {suggestions['analysis']}\n")

    edges = suggestions.get("suggested_edges", [])
    print(f"  SUGGESTED EDGES  ({len(edges)})\n")
    for e in edges:
        print(f"    ─▶ {e['name']}")
        print(f"       {e['source_table']}.{e['source_column']}  →  {e['target_table']}")
        print(f"       {e['rationale']}")

    corrections = suggestions.get("field_type_corrections", [])
    print(f"\n  FIELD TYPE CORRECTIONS  ({len(corrections)})\n")
    for c in corrections:
        print(f"    {c['table']}.{c['column']}")
        print(f"       {c['current_type']}  →  {c['suggested_type']}  ({c['rationale']})")

    reclassify = suggestions.get("reclassify_as_edge", [])
    print(f"\n  RECLASSIFY AS EDGE  ({len(reclassify)})\n")
    for r in reclassify:
        print(f"    {r['table']}  →  edge between {r['source_table']} and {r['target_table']}")
        print(f"       {r['rationale']}")

    warnings = suggestions.get("warnings", [])
    if warnings:
        print(f"\n  WARNINGS\n")
        for w in warnings:
            print(f"    ⚠  {w}")

    print("═" * W + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — apply suggestions to the manifest dict
# ─────────────────────────────────────────────────────────────────────────────

def apply_suggestions(manifest: dict, suggestions: dict, col_meta: dict) -> dict:
    import copy
    m = copy.deepcopy(manifest)

    core = m.setdefault("schema", {}).setdefault("core_schema", {})
    edge_cfg = core.setdefault("edge_config", {})
    vertices = core.setdefault("vertex_config", {}).setdefault("vertices", [])

    # build a quick index: table_name → vertex entry
    vtx_index = {to_str(v.get("name", "")): v for v in vertices}

    # ── apply suggested_edges ───────────────────────────────────────────────
    for e in suggestions.get("suggested_edges", []):
        name    = e["name"]
        src     = e["source_table"]
        tgt     = e["target_table"]
        src_col = e["source_column"]

        if name in edge_cfg:
            print(f"  [skip] edge '{name}' already exists")
            continue
        if src not in vtx_index or tgt not in vtx_index:
            print(f"  [skip] edge '{name}' — source or target not in manifest")
            continue

        # look up actual vertex name tuples so GraFlo is happy
        src_full = vtx_index[src]["name"]
        tgt_full = vtx_index[tgt]["name"]

        edge_cfg[name] = {
            "name":          name,
            "source_vertex": src_full,
            "target_vertex": tgt_full,
            "fields":        [],
            "_added_by_llm": True,
            "_via_column":   src_col,
            "_rationale":    e.get("rationale", ""),
        }
        print(f"  [add edge] {name}: {src} → {tgt}  (via {src_col})")

    # ── apply field_type_corrections ────────────────────────────────────────
    for corr in suggestions.get("field_type_corrections", []):
        tbl  = corr["table"]
        col  = corr["column"]
        ntype = corr["suggested_type"]
        vtx  = vtx_index.get(tbl)
        if not vtx:
            print(f"  [skip] type correction for '{tbl}.{col}' — table not in manifest")
            continue
        for field in vtx.get("fields", []):
            if isinstance(field, dict) and to_str(field.get("name", "")) == col:
                old = field.get("field_type", "?")
                field["field_type"] = ntype
                print(f"  [fix type] {tbl}.{col}: {old} → {ntype}")
                break

    # ── apply reclassify_as_edge ─────────────────────────────────────────────
    for r in suggestions.get("reclassify_as_edge", []):
        tbl = r["table"]
        if tbl not in vtx_index:
            print(f"  [skip] reclassify '{tbl}' — not in manifest vertices")
            continue
        src = r.get("source_table", "")
        tgt = r.get("target_table", "")
        if src not in vtx_index or tgt not in vtx_index:
            print(f"  [skip] reclassify '{tbl}' — endpoints not in manifest")
            continue

        # move from vertices to edge_config
        vtx_entry  = vtx_index[tbl]
        src_full   = vtx_index[src]["name"]
        tgt_full   = vtx_index[tgt]["name"]
        edge_cfg[tbl] = {
            "name":          tbl,
            "source_vertex": src_full,
            "target_vertex": tgt_full,
            "fields":        vtx_entry.get("fields", []),
            "_added_by_llm": True,
            "_rationale":    r.get("rationale", ""),
        }
        # remove from vertices list
        core["vertex_config"]["vertices"] = [
            v for v in vertices if to_str(v.get("name", "")) != tbl
        ]
        print(f"  [reclassify] '{tbl}' moved from vertex → edge ({src} → {tgt})")

    return m


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print suggestions but do not apply them or save files")
    args = parser.parse_args()

    prefixes = parse_prefixes()

    # 1. load manifest
    print("\n[1/4] Loading manifest …")
    manifest = load_manifest()

    # 2. fetch column metadata from postgres
    print("[2/4] Fetching column metadata from PostgreSQL …")
    col_meta = fetch_column_metadata(prefixes)
    print(f"      {len(col_meta)} tables loaded")

    # 3. build compact summary
    print("[3/4] Building schema summary …")
    summary = build_schema_summary(manifest, col_meta)

    # 4. call LLM
    print("[4/4] Asking LLM to analyse the schema …")
    suggestions = call_llm(summary)

    # save raw suggestions always (useful for review)
    if not args.dry_run:
        sug_path = Path("llm-suggestions.json")
        with open(sug_path, "w") as fh:
            json.dump(suggestions, fh, indent=2)
        print(f"      Suggestions saved → {sug_path}")

    # print human-readable summary
    print_suggestions(suggestions)

    if args.dry_run:
        print("  [dry-run] No files written.")
        return

    # apply suggestions
    print("  Applying suggestions …\n")
    enriched = apply_suggestions(manifest, suggestions, col_meta)

    out = Path("enriched-manifest.yaml")
    with open(out, "w") as fh:
        yaml.safe_dump(enriched, fh, default_flow_style=False, sort_keys=False)

    print(f"\n  Enriched manifest saved → {out}")
    print("  Review it, then run:\n")
    print("    MANIFEST=enriched-manifest.yaml python ingest.py\n")


if __name__ == "__main__":
    main()