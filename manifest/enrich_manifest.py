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
    python enrich_manifest.py
    python enrich_manifest.py --dry-run
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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_prefixes() -> list[str]:
    raw = os.getenv("TABLE_PREFIXES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()]


def to_str(name) -> str:
    """Normalize GraFlo name (tuple/list/str) → plain table name string."""
    if isinstance(name, (tuple, list)):
        return str(name[-1])
    return str(name).strip()


def normalize(s: str) -> str:
    """Lowercase + strip for fuzzy matching."""
    return s.strip().lower()


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
#           NOTE: no prefix filter here — we want the LLM to see ALL tables
#           so it can spot relationships between apm_ and non-apm_ tables too.
#           The manifest already limits what gets ingested.
# ─────────────────────────────────────────────────────────────────────────────

PG_COLUMNS_SQL = """
SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.is_nullable,
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

def fetch_column_metadata() -> dict[str, list[dict]]:
    """
    Returns {table_name: [{column, type, pk, fk, nullable}, ...]}
    No prefix filter — full schema context helps the LLM find cross-table links.
    """
    uri      = os.environ["POSTGRES_URI"]
    host_str = uri.split("//")[-1]
    host     = host_str.split(":")[0].split("/")[0]
    port     = int(host_str.split(":")[-1]) if ":" in host_str else 5432
    schema   = os.getenv("POSTGRES_SCHEMA", "public")

    conn = psycopg2.connect(
        host=host, port=port,
        user=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DATABASE"],
    )
    with conn.cursor() as cur:
        cur.execute(PG_COLUMNS_SQL, {"schema": schema})
        rows = cur.fetchall()
    conn.close()

    tables: dict[str, list[dict]] = {}
    for table_name, col, dtype, nullable, pk_flag, fk_flag in rows:
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

def extract_vertices(manifest: dict) -> tuple[list, set[str]]:
    """Returns (vertex_list, set_of_plain_names)."""
    vertices = (manifest.get("schema", {})
                        .get("core_schema", {})
                        .get("vertex_config", {})
                        .get("vertices", []))
    names = {to_str(v.get("name", "")) for v in vertices}
    return vertices, names


def build_schema_summary(manifest: dict, col_meta: dict[str, list[dict]]) -> str:
    lines = []
    vertices, vertex_names = extract_vertices(manifest)

    # ── CRITICAL: give the LLM the exact names it must use ──────────────────
    lines.append("## IMPORTANT: EXACT VERTEX NAMES IN THIS MANIFEST")
    lines.append("## You MUST use these exact strings in source_table and target_table.")
    lines.append("## Do not invent or alter names.\n")
    for name in sorted(vertex_names):
        lines.append(f"  - {name}")

    # ── current vertices with fields ─────────────────────────────────────────
    lines.append("\n## VERTEX DETAIL (tables already in the graph manifest)")
    for v in sorted(vertices, key=lambda x: to_str(x.get("name", ""))):
        name   = to_str(v.get("name", ""))
        fields = v.get("fields", []) or []
        ids    = v.get("identity_fields", []) or []
        id_cols = [to_str(i.get("name", i) if isinstance(i, dict) else i) for i in ids]
        f_cols  = [
            f"{to_str(f.get('name', f) if isinstance(f, dict) else f)}"
            f"({f.get('field_type','?') if isinstance(f, dict) else '?'})"
            for f in fields
        ]
        lines.append(f"  {name}:")
        if id_cols:
            lines.append(f"    identity : {', '.join(id_cols)}")
        if f_cols:
            lines.append(f"    fields   : {', '.join(f_cols[:10])}"
                         + (" …" if len(f_cols) > 10 else ""))

    # ── current edges ────────────────────────────────────────────────────────
    edges = manifest.get("schema", {}).get("core_schema", {}).get("edge_config", {}) or {}
    lines.append(f"\n## CURRENT EDGES ({len(edges)} found via FK constraints)")
    if edges:
        for ename, e in edges.items():
            src = to_str(e.get("source_vertex", "?"))
            tgt = to_str(e.get("target_vertex", "?"))
            lines.append(f"  {to_str(ename)}: {src} → {tgt}")
    else:
        lines.append("  (none — no FK constraints declared in the database)")

    # ── column detail: only manifest tables + tables that reference them ─────
    # Show ALL columns for manifest tables, but also show any other table that
    # has a column pointing to a manifest table (these are the missing edges).
    lines.append("\n## COLUMN DETAIL — manifest tables + tables referencing them")
    lines.append("## Columns ending in _id/_fk/_ref with no FK constraint = likely missing edges\n")

    implicit_fks = []
    for table in sorted(col_meta.keys()):
        cols = col_meta[table]
        is_manifest_table = table in vertex_names
        # also include non-manifest tables that have _id columns pointing to manifest tables
        has_ref_to_manifest = any(
            (c["column"].endswith("_id") or c["column"].endswith("_fk"))
            and not c["pk"] and not c["fk"]
            and c["column"].replace("_id", "").replace("_fk", "") in vertex_names
            for c in cols
        )
        if not is_manifest_table and not has_ref_to_manifest:
            continue

        col_lines = []
        for c in cols:
            flags = []
            if c["pk"]:  flags.append("PK")
            if c["fk"]:  flags.append(c["fk"])
            if c["nullable"]: flags.append("nullable")
            flag_str = f" [{', '.join(flags)}]" if flags else ""

            is_implicit = (
                not c["fk"] and not c["pk"]
                and (c["column"].endswith("_id") or c["column"].endswith("_fk")
                     or c["column"].endswith("_ref"))
            )
            marker = " ← POSSIBLE IMPLICIT FK" if is_implicit else ""
            col_lines.append(f"    {c['column']} ({c['type']}){flag_str}{marker}")
            if is_implicit:
                implicit_fks.append((table, c["column"]))

        label = "(IN MANIFEST)" if is_manifest_table else "(NOT IN MANIFEST — references manifest table)"
        lines.append(f"  {table} {label}:")
        lines.extend(col_lines)

    # ── implicit FK summary ──────────────────────────────────────────────────
    if implicit_fks:
        lines.append("\n## IMPLICIT FK SUMMARY — most likely missing relationships:")
        for table, col in implicit_fks:
            guess = col.removesuffix("_id").removesuffix("_fk").removesuffix("_ref")
            in_manifest = "✓ in manifest" if guess in vertex_names else "✗ not in manifest"
            lines.append(f"  {table}.{col}  →  probably references '{guess}' ({in_manifest})")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — call the LLM
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a database-to-knowledge-graph schema expert.
You will receive a summary of a PostgreSQL schema and a GraFlo graph manifest.

CRITICAL RULES:
1. In suggested_edges, source_table and target_table MUST be taken VERBATIM from
   the "EXACT VERTEX NAMES IN THIS MANIFEST" list. Do not modify, abbreviate, or invent names.
2. source_column must be a real column that exists in source_table's column list.
3. field_type values must be one of: INT, STRING, FLOAT, DATETIME, BOOL
4. If uncertain, add to warnings rather than guessing.
5. Keep rationale to one sentence.

Respond ONLY with valid JSON — no markdown fences, no explanation, no preamble.

{
  "analysis": "2-3 sentence overall assessment",
  "suggested_edges": [
    {
      "name": "snake_case_edge_label",
      "source_table": "exact_name_from_manifest_list",
      "source_column": "the_column_holding_the_reference",
      "target_table": "exact_name_from_manifest_list",
      "rationale": "one sentence"
    }
  ],
  "field_type_corrections": [
    {
      "table": "exact_name_from_manifest_list",
      "column": "column_name",
      "current_type": "STRING",
      "suggested_type": "DATETIME",
      "rationale": "one sentence"
    }
  ],
  "reclassify_as_edge": [
    {
      "table": "exact_name_from_manifest_list",
      "source_table": "exact_name_from_manifest_list",
      "target_table": "exact_name_from_manifest_list",
      "rationale": "one sentence"
    }
  ],
  "warnings": ["string"]
}"""


def call_llm(schema_summary: str) -> dict:
    import urllib.request

    base_url = os.environ.get("LLM_API_URL", "").rstrip("/")
    api_key  = os.environ.get("LLM_API_KEY", "")
    model    = os.environ.get("LLM_MODEL", "gpt-4o")

    if not base_url:
        print("[error] LLM_API_URL not set in .env")
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
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"},
        method="POST",
    )

    print(f"  Calling {base_url}  (model: {model}) …")
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())

    raw = body["choices"][0]["message"]["content"].strip()
    # strip accidental markdown fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0]

    return json.loads(raw)


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — print suggestions
# ─────────────────────────────────────────────────────────────────────────────

def print_suggestions(suggestions: dict) -> None:
    W = 64
    print("\n" + "═" * W)
    print("  LLM manifest analysis")
    print("═" * W)
    if suggestions.get("analysis"):
        print(f"\n  {suggestions['analysis']}\n")

    for e in suggestions.get("suggested_edges", []):
        print(f"  ─▶ {e['name']}")
        print(f"     {e['source_table']}.{e['source_column']}  →  {e['target_table']}")
        print(f"     {e['rationale']}")

    corrections = suggestions.get("field_type_corrections", [])
    if corrections:
        print(f"\n  TYPE CORRECTIONS\n")
        for c in corrections:
            print(f"  {c['table']}.{c['column']}: {c['current_type']} → {c['suggested_type']}")

    reclassify = suggestions.get("reclassify_as_edge", [])
    if reclassify:
        print(f"\n  RECLASSIFY AS EDGE\n")
        for r in reclassify:
            print(f"  {r['table']} → edge({r['source_table']} → {r['target_table']})")

    for w in suggestions.get("warnings", []):
        print(f"  ⚠  {w}")
    print("═" * W + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — apply suggestions
# ─────────────────────────────────────────────────────────────────────────────

def build_vtx_index(manifest: dict) -> tuple[dict, dict]:
    """
    Returns:
      exact_index  — {exact_name: vertex_entry}     for direct lookup
      fuzzy_index  — {normalized_name: exact_name}  for fallback matching
    """
    vertices = (manifest.get("schema", {})
                        .get("core_schema", {})
                        .get("vertex_config", {})
                        .get("vertices", []))
    exact = {}
    fuzzy = {}
    for v in vertices:
        key = to_str(v.get("name", ""))
        exact[key] = v
        fuzzy[normalize(key)] = key
    return exact, fuzzy


def resolve_name(name: str, exact: dict, fuzzy: dict) -> str | None:
    """
    Try exact match first, then case-insensitive fuzzy match.
    Returns the exact key if found, None otherwise.
    """
    name = name.strip()
    if name in exact:
        return name
    norm = normalize(name)
    if norm in fuzzy:
        matched = fuzzy[norm]
        print(f"  [fuzzy match] '{name}' → '{matched}'")
        return matched
    return None


def apply_suggestions(manifest: dict, suggestions: dict) -> dict:
    import copy
    m = copy.deepcopy(manifest)

    core    = m.setdefault("schema", {}).setdefault("core_schema", {})
    edge_cfg = core.setdefault("edge_config", {})

    exact_idx, fuzzy_idx = build_vtx_index(m)

    # ── debug: show available names so user can spot mismatches ──────────────
    print(f"\n  [debug] {len(exact_idx)} vertices in manifest:")
    for name in sorted(exact_idx.keys()):
        print(f"           {name}")
    print()

    # ── suggested_edges ───────────────────────────────────────────────────────
    for e in suggestions.get("suggested_edges", []):
        edge_name = e["name"]
        src_raw   = e.get("source_table", "")
        tgt_raw   = e.get("target_table", "")
        src_col   = e.get("source_column", "")

        src = resolve_name(src_raw, exact_idx, fuzzy_idx)
        tgt = resolve_name(tgt_raw, exact_idx, fuzzy_idx)

        if edge_name in edge_cfg:
            print(f"  [skip] '{edge_name}' already exists")
            continue

        if src is None or tgt is None:
            missing = []
            if src is None: missing.append(f"source='{src_raw}'")
            if tgt is None: missing.append(f"target='{tgt_raw}'")
            print(f"  [skip] '{edge_name}' — {' and '.join(missing)} not in manifest")
            print(f"         available names: {', '.join(sorted(exact_idx.keys()))}")
            continue

        src_full = exact_idx[src]["name"]
        tgt_full = exact_idx[tgt]["name"]

        edge_cfg[edge_name] = {
            "name":          edge_name,
            "source_vertex": src_full,
            "target_vertex": tgt_full,
            "fields":        [],
            "_added_by_llm": True,
            "_via_column":   src_col,
            "_rationale":    e.get("rationale", ""),
        }
        print(f"  [add edge] {edge_name}: {src} → {tgt}  (via {src_col})")

    # ── field_type_corrections ────────────────────────────────────────────────
    for corr in suggestions.get("field_type_corrections", []):
        tbl   = resolve_name(corr.get("table", ""), exact_idx, fuzzy_idx)
        col   = corr.get("column", "")
        ntype = corr.get("suggested_type", "")
        if tbl is None:
            print(f"  [skip] type fix '{corr.get('table')}.{col}' — table not in manifest")
            continue
        vtx = exact_idx[tbl]
        for field in vtx.get("fields", []):
            if isinstance(field, dict) and to_str(field.get("name", "")) == col:
                old = field.get("field_type", "?")
                field["field_type"] = ntype
                print(f"  [fix type] {tbl}.{col}: {old} → {ntype}")
                break

    # ── reclassify_as_edge ────────────────────────────────────────────────────
    for r in suggestions.get("reclassify_as_edge", []):
        tbl = resolve_name(r.get("table", ""), exact_idx, fuzzy_idx)
        src = resolve_name(r.get("source_table", ""), exact_idx, fuzzy_idx)
        tgt = resolve_name(r.get("target_table", ""), exact_idx, fuzzy_idx)
        if not all([tbl, src, tgt]):
            print(f"  [skip] reclassify '{r.get('table')}' — could not resolve all names")
            continue
        vtx_entry = exact_idx[tbl]
        edge_cfg[tbl] = {
            "name":          tbl,
            "source_vertex": exact_idx[src]["name"],
            "target_vertex": exact_idx[tgt]["name"],
            "fields":        vtx_entry.get("fields", []),
            "_added_by_llm": True,
            "_rationale":    r.get("rationale", ""),
        }
        # remove from vertices
        verts = core["vertex_config"]["vertices"]
        core["vertex_config"]["vertices"] = [
            v for v in verts if to_str(v.get("name", "")) != tbl
        ]
        print(f"  [reclassify] '{tbl}' → edge ({src} → {tgt})")

    return m


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show suggestions but do not write files")
    args = parser.parse_args()

    manifest_path = Path(os.getenv("MANIFEST", "generated-manifest.yaml"))

    print(f"\n[1/4] Loading manifest from {manifest_path} …")
    manifest = load_manifest(manifest_path)
    _, vertex_names = extract_vertices(manifest)
    print(f"      {len(vertex_names)} vertices in manifest")

    print("[2/4] Fetching column metadata from PostgreSQL (all tables) …")
    col_meta = fetch_column_metadata()
    print(f"      {len(col_meta)} tables loaded from DB")

    print("[3/4] Building schema summary for LLM …")
    summary = build_schema_summary(manifest, col_meta)

    print("[4/4] Asking LLM …")
    suggestions = call_llm(summary)

    if not args.dry_run:
        sug_path = Path("llm-suggestions.json")
        with open(sug_path, "w") as fh:
            json.dump(suggestions, fh, indent=2)
        print(f"      Raw suggestions saved → {sug_path}")

    print_suggestions(suggestions)

    if args.dry_run:
        print("  [dry-run] No files written.")
        return

    print("  Applying suggestions …\n")
    enriched = apply_suggestions(manifest, suggestions)

    out = Path("enriched-manifest.yaml")
    with open(out, "w") as fh:
        yaml.safe_dump(enriched, fh, default_flow_style=False, sort_keys=False)

    print(f"\n  Enriched manifest saved → {out}")
    print("  Next step:")
    print("    MANIFEST=enriched-manifest.yaml INGEST_LIMIT=50 python ingest.py\n")


if __name__ == "__main__":
    main()
