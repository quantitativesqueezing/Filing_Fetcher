#!/usr/bin/env python3
import sys
import json
import time
from sec_feed.parser import fetch_index_documents, to_ndjson

def main(urls):
    payloads = []
    for u in urls:
        payload = fetch_index_documents(u)
        payloads.append(payload)
        time.sleep(0.6)
    print(json.dumps(payloads, indent=2))
    try:
        print("\n# NDJSON (one line per document):", file=sys.stderr)
        for p in payloads:
            for line in to_ndjson(p):
                print(line, file=sys.stderr)
    except Exception:
        pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: fetch_sec_docs.py <index-url> [<index-url> ...]", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1:])
