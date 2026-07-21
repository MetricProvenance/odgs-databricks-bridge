#!/usr/bin/env python3
"""Minimal JWKS endpoint for SEGMENT 3 (Databricks) pack-signature verification."""
import json, os
from flask import Flask, jsonify

app = Flask(__name__)
PORT = 8601
KEYS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys")


@app.route("/jwks.json")
def jwks():
    path = os.path.join(KEYS_DIR, "jwks.json")
    if not os.path.exists(path):
        return jsonify({"keys": []}), 404
    with open(path) as f:
        return jsonify(json.load(f))


if __name__ == "__main__":
    print(f"Databricks JWKS endpoint on http://localhost:{PORT}")
    app.run(host="127.0.0.1", port=PORT)
