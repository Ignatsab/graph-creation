"""
enrich_manifest.py — LLM-powered manifest enrichment
=====================================================
Usage:
    python enrich_manifest.py
    python enrich_manifest.py --dry-run
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

def to_str(name) -> str:
    if isinstance(name, (tuple, list)):
        return str(name[-1])
    return str(name).strip()


def normalize(s: str) -> str:
    return s.strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
# Manifest loading — auto-detect structure
# ─────────────────────────────────────────────────────────────────────────────

def find_key(obj, target_key: str, _path: str = "") -> tuple:
    """
    Recursively search any nested dict/list for target_key.
    Returns (value, dotted_path) or (None, "").
    Works regardless of how GraFlo serialised the YAML.
    """
    if isinstance(obj, dict):
        if target_key in obj and obj[target_key] is not None:
            return obj[target_key], f"{_path}.{target_key}".lstrip(".")
        for k, v in obj.items():
            val, path = find_key(v, target_key, f"{_path}.{k}".lstrip("."))
            if val is not None:
                return val, path
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            val, path = find_key(item, target_key, f"{_path}[{i}]")
            if val is not None:
                return val, path
    return None, ""


def load_manifest(path: Path) -> dict:
    if not path.exists():
        print(f"[error] {path} not found — run inspect_schema.py first.")
        sys.exit(1)
    with open(path) as fh:
        return yaml.safe_load(fh)


def extract_vertices(manifest: dict) -> tuple[list, set[str]]:
    """
    Find the vertices list anywhere in the YAML, report the path found.
    Returns (vertex_list, set_of_plain_names).
    """
    vertices, path = find_key(manifest, "vertices")
    if not vertices:
        # fallback: look for any list whose items have a "name" and "fields" key
        vertices, path = _find_vertex_list(manifest)

    if vertices:
        print(f"  [manifest] vertices found at: {path}  ({len(vertices)} items)")
    else:
        print("  [manifest] WARNING: could not find vertices in YAML")
        print("             top-level keys:", list(manifest.keys()))
        vertices = []

    names = {to_str(v.get("name", "")) for v in vertices if isinstance(v, dict)}
    return vertices, names


def extract_edges(manifest: dict) -> dict:
    """Find edge_config dict anywhere in the YAML."""
    for key in ["edge_config", "edges", "edge_configs"]:
        val, path = find_key(manifest, key)
        if isinstance(val, dict) and val:
            print(f"  [manifest] edges found at: {path}  ({len(val)} items)")
            return val
    print("  [manifest] no edges found in YAML")
    return {}


def _find_vertex_list(manifest: dict) -> tuple:
    """
    Fallback: find any list whose first item looks like a vertex
    (has 'name' and either 'fields' or 'identity_fields').
    """
    def search(obj, path):
        if isinstance(obj, list) and obj:
            first = obj[0]
            if isinstance(first, dict) and "name" in first and (
                "fields" in first or "identity_fields" in first
            ):
                return obj, path
        if isinstance(obj, dict):
            for k, v in obj.items():
                result, p = search(v, f"{path}.{k}".lstrip("."))
                if result is not None:
                    return result, p
        return None, ""
    return search(manifest, "")


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL column metadata
# ─────────────────────────────────────────────────────────────────────────────

PG_COLUMNS_SQL = """
SELECT
    c.table_name, c.column_name, c.data_type,
    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_pk,
    fk.foreign_table
FROM information_schema.columns c
LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema   = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %(s)s
) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name, ccu.table_name AS foreign_table
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %(s)s
) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
WHERE c.table_schema = %(s)s
ORDER BY c.table_name, c.ordinal_position
"""

def fetch_column_metadata() -> dict[str, list[dict]]:
    uri      = os.environ["POSTGRES_URI"]
    host_str = uri.split("//")[-1]
    host     = host_str.split(":")[0].split("/")[0]
    port     = int(host_str.split(":")[-1].split("/")[0]) if ":" in host_str else 5432
    schema   = os.getenv("POSTGRES_SCHEMA", "public")

    conn = psycopg2.connect(
        host=host, port=port,
        user=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DATABASE"],
    )
    with conn.cursor() as cur:
        cur.execute(PG_COLUMNS_SQL, {"s": schema})
        rows = cur.fetchall()
    conn.close()

    tables: dict[str, list[dict]] = {}
    for table_name, col, dtype, is_pk, foreign_table in rows:
        tables.setdefault(table_name, []).append({
            "column": col, "type": dtype,
            "pk": is_pk, "fk_to": foreign_table,
        })
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# Build compact LLM prompt — optimised for smaller context windows
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(vertex_names: set[str], edges: dict,
                 col_meta: dict[str, list[dict]]) -> str:
    """
    Deliberately compact — three sections only:
      1. Exact vertex names the LLM must use
      2. Already-known edges
      3. Implicit FK candidates (columns ending _id/_fk with no constraint)
    No full column dumps — keeps the prompt small for limited-context models.
    """
    lines = []

    # ── 1. Vertex names — the LLM must copy these exactly ───────────────────
    lines.append("VERTICES (use these exact names in your JSON):")
    for n in sorted(vertex_names):
        lines.append(f"  {n}")

    # ── 2. Known edges ───────────────────────────────────────────────────────
    lines.append(f"\nKNOWN EDGES ({len(edges)}):")
    if edges:
        for ename, e in edges.items():
            src = to_str(e.get("source_vertex", e.get("source", "?")))
            tgt = to_str(e.get("target_vertex", e.get("target", "?")))
            lines.append(f"  {to_str(ename)}: {src} → {tgt}")
    else:
        lines.append("  (none)")

    # ── 3. Implicit FK candidates — only for vertex tables ──────────────────
    lines.append("\nIMPLICIT FK CANDIDATES (columns that look like missing relationships):")
    found_any = False
    for table in sorted(vertex_names):           # only manifest tables
        cols = col_meta.get(table, [])
        candidates = [
            c for c in cols
            if not c["pk"] and not c["fk_to"]
            and (c["column"].endswith("_id") or c["column"].endswith("_fk")
                 or c["column"].endswith("_ref"))
        ]
        if candidates:
            found_any = True
            for c in candidates:
                guess = (c["column"]
                         .removesuffix("_id")
                         .removesuffix("_fk")
                         .removesuffix("_ref"))
                in_manifest = guess in vertex_names
                lines.append(
                    f"  {table}.{c['column']} ({c['type']})"
                    f"  →  probably references '{guess}'"
                    f"  {'[IN MANIFEST]' if in_manifest else '[NOT IN MANIFEST]'}"
                )

    # also check non-manifest tables that reference manifest tables
    lines.append("\nNON-MANIFEST TABLES REFERENCING MANIFEST TABLES:")
    found_external = False
    for table, cols in sorted(col_meta.items()):
        if table in vertex_names:
            continue
        refs = [
            c for c in cols
            if not c["pk"] and not c["fk_to"]
            and (c["column"].endswith("_id") or c["column"].endswith("_fk"))
            and c["column"].removesuffix("_id").removesuffix("_fk") in vertex_names
        ]
        if refs:
            found_external = True
            for c in refs:
                lines.append(f"  {table}.{c['column']} → manifest table '{c['column'].removesuffix('_id').removesuffix('_fk')}'")

    if not found_any:
        lines.append("  (none found — tables may lack _id naming conventions)")
    if not found_external:
        lines.append("  (none)")

    # ── Type anomalies — only flag obvious mismatches ────────────────────────
    lines.append("\nPOSSIBLE TYPE MISMATCHES (in manifest vertex tables):")
    type_issues = []
    for table in sorted(vertex_names):
        for c in col_meta.get(table, []):
            pg_type = c["type"].lower()
            if any(t in pg_type for t in ["timestamp", "date", "time"]):
                type_issues.append(f"  {table}.{c['column']} is {c['type']} — should be DATETIME not STRING")
            elif "bool" in pg_type:
                type_issues.append(f"  {table}.{c['column']} is BOOLEAN — should be BOOL")
            elif any(t in pg_type for t in ["numeric", "decimal", "real", "double"]):
                type_issues.append(f"  {table}.{c['column']} is {c['type']} — should be FLOAT")
    if type_issues:
        lines.extend(type_issues[:20])   # cap at 20 to save context
    else:
        lines.append("  (none detected)")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# LLM call
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a graph schema expert. Given a list of graph vertices, known edges, and \
implicit FK candidates from a PostgreSQL database, suggest missing edges and field \
type corrections.

RULES:
- source_table and target_table must be EXACTLY from the VERTICES list. No changes.
- source_column must exist in source_table (it will be in the FK CANDIDATES section).
- field_type must be one of: INT, STRING, FLOAT, DATETIME, BOOL
- Only suggest confident relationships. If unsure, skip it.

Reply with ONLY a JSON object, no markdown, no explanation:
{
  "suggested_edges": [
    {"name": "snake_case", "source_table": "exact", "source_column": "col", "target_table": "exact", "rationale": "one sentence"}
  ],
  "field_type_corrections": [
    {"table": "exact", "column": "col", "current_type": "STRING", "suggested_type": "DATETIME"}
  ],
  "warnings": ["string"]
}"""


def call_llm(prompt: str) -> dict:
    import urllib.request

    base_url = os.environ.get("LLM_API_URL", "").rstrip("/")
    api_key  = os.environ.get("LLM_API_KEY", "")
    model    = os.environ.get("LLM_MODEL", "gpt-4o")

    if not base_url:
        print("[error] LLM_API_URL not set")
        sys.exit(1)

    payload = json.dumps({
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
    }).encode()

    req = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=payload,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    print(f"  Calling {base_url} (model: {model}) …")
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())

    raw = body["choices"][0]["message"]["content"].strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0]
    return json.loads(raw)


# ─────────────────────────────────────────────────────────────────────────────
# Apply suggestions
# ─────────────────────────────────────────────────────────────────────────────

def build_vtx_index(manifest: dict) -> tuple[dict, dict]:
    vertices, _ = extract_vertices(manifest)
    exact = {to_str(v.get("name", "")): v for v in vertices if isinstance(v, dict)}
    fuzzy = {normalize(k): k for k in exact}
    return exact, fuzzy


def resolve(name: str, exact: dict, fuzzy: dict) -> str | None:
    name = name.strip()
    if name in exact:
        return name
    n = normalize(name)
    if n in fuzzy:
        print(f"  [fuzzy] '{name}' → '{fuzzy[n]}'")
        return fuzzy[n]
    return None


def find_edge_config(manifest: dict) -> dict:
    """Find and return the edge_config dict (mutable reference)."""
    edges = extract_edges(manifest)
    # If we got a value, we need the mutable reference inside manifest
    # Re-locate it by path
    for key in ["edge_config", "edges", "edge_configs"]:
        val, path = find_key(manifest, key)
        if isinstance(val, dict):
            return val
    # Not found — create one at the most likely path
    core = (manifest.setdefault("schema", {})
                    .setdefault("core_schema", {}))
    core.setdefault("edge_config", {})
    return core["edge_config"]


def apply_suggestions(manifest: dict, suggestions: dict) -> dict:
    import copy
    m = copy.deepcopy(manifest)

    exact_idx, fuzzy_idx = build_vtx_index(m)
    edge_cfg = find_edge_config(m)

    print(f"\n  [debug] {len(exact_idx)} vertices available:")
    for name in sorted(exact_idx):
        print(f"           {name}")
    print()

    applied = 0

    for e in suggestions.get("suggested_edges", []):
        ename   = e.get("name", "")
        src_raw = e.get("source_table", "")
        tgt_raw = e.get("target_table", "")
        src_col = e.get("source_column", "")

        src = resolve(src_raw, exact_idx, fuzzy_idx)
        tgt = resolve(tgt_raw, exact_idx, fuzzy_idx)

        if ename in edge_cfg:
            print(f"  [skip] '{ename}' already exists")
            continue
        if src is None or tgt is None:
            bad = f"source='{src_raw}'" if src is None else f"target='{tgt_raw}'"
            print(f"  [skip] '{ename}' — {bad} not found in manifest")
            continue

        edge_cfg[ename] = {
            "name":          ename,
            "source_vertex": exact_idx[src]["name"],
            "target_vertex": exact_idx[tgt]["name"],
            "fields":        [],
            "_llm":          True,
            "_via":          src_col,
            "_why":          e.get("rationale", ""),
        }
        print(f"  [add] {ename}: {src} → {tgt}  (via {src_col})")
        applied += 1

    for corr in suggestions.get("field_type_corrections", []):
        tbl   = resolve(corr.get("table", ""), exact_idx, fuzzy_idx)
        col   = corr.get("column", "")
        ntype = corr.get("suggested_type", "")
        if not tbl:
            continue
        for field in exact_idx[tbl].get("fields", []):
            if isinstance(field, dict) and to_str(field.get("name", "")) == col:
                old = field.get("field_type", "?")
                field["field_type"] = ntype
                print(f"  [type] {tbl}.{col}: {old} → {ntype}")
                applied += 1
                break

    print(f"\n  {applied} change(s) applied.")
    return m


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    manifest_path = Path(os.getenv("MANIFEST", "generated-manifest.yaml"))

    print(f"\n[1/4] Loading manifest: {manifest_path}")
    manifest = load_manifest(manifest_path)
    vertices, vertex_names = extract_vertices(manifest)
    edges = extract_edges(manifest)
    print(f"      {len(vertex_names)} vertices, {len(edges)} edges")

    if not vertex_names:
        print("\n[error] No vertices found — printing YAML top-level structure for debugging:")
        def show_keys(obj, prefix="", depth=0):
            if depth > 3: return
            if isinstance(obj, dict):
                for k, v in obj.items():
                    t = type(v).__name__
                    n = f"({len(v)})" if isinstance(v, (dict,list)) else ""
                    print(f"  {prefix}{k}: [{t}]{n}")
                    show_keys(v, prefix+"  ", depth+1)
            elif isinstance(obj, list) and obj:
                print(f"  {prefix}[0]: [{type(obj[0]).__name__}]")
                show_keys(obj[0], prefix+"  ", depth+1)
        show_keys(manifest)
        sys.exit(1)

    print("\n[2/4] Fetching column metadata from PostgreSQL …")
    col_meta = fetch_column_metadata()
    print(f"      {len(col_meta)} tables")

    print("\n[3/4] Building compact prompt …")
    prompt = build_prompt(vertex_names, edges, col_meta)
    print(f"      ~{len(prompt.split())} words in prompt")

    print("\n[4/4] Calling LLM …")
    suggestions = call_llm(prompt)

    if not args.dry_run:
        with open("llm-suggestions.json", "w") as fh:
            json.dump(suggestions, fh, indent=2)
        print("      saved → llm-suggestions.json")

    # print summary
    print(f"\n  Suggested edges     : {len(suggestions.get('suggested_edges', []))}")
    print(f"  Type corrections    : {len(suggestions.get('field_type_corrections', []))}")
    for w in suggestions.get("warnings", []):
        print(f"  ⚠  {w}")

    if args.dry_run:
        print("\n  [dry-run] not writing files.")
        print("  Raw LLM output:")
        print(json.dumps(suggestions, indent=2))
        return

    print("\n  Applying …")
    enriched = apply_suggestions(manifest, suggestions)

    out = Path("enriched-manifest.yaml")
    with open(out, "w") as fh:
        yaml.safe_dump(enriched, fh, default_flow_style=False, sort_keys=False)
    print(f"\n  Saved → {out}")
    print("  Next: MANIFEST=enriched-manifest.yaml INGEST_LIMIT=50 python ingest.py\n")


if __name__ == "__main__":
    main()
