"""
debug_manifest.py — print the actual structure of generated-manifest.yaml
Run this once to see what keys GraFlo actually serialises to.
"""
import sys, yaml
from pathlib import Path

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("generated-manifest.yaml")
data = yaml.safe_load(open(path))

def show(obj, prefix="", depth=0):
    if depth > 4:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            kind = type(v).__name__
            n    = f"({len(v)} items)" if isinstance(v, (dict, list)) else ""
            print(f"{prefix}{k}:  [{kind}] {n}")
            if isinstance(v, (dict, list)) and depth < 3:
                show(v, prefix + "  ", depth + 1)
    elif isinstance(obj, list):
        print(f"{prefix}[list of {len(obj)}]")
        if obj and depth < 3:
            print(f"{prefix}  first item:")
            show(obj[0], prefix + "    ", depth + 1)

show(data)
