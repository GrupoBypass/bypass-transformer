#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify
import subprocess
import json
import os
import sys

app = Flask(__name__)

@app.route("/process-test", methods=["POST"])
def process_test():
    data = request.json or {}
    print("Teste recebido:", data)
    return jsonify({"status": "ok", "payload": data})

@app.route("/process", methods=["POST"])
def process_file():
    try:
        data = request.json or {}
        key = data.get("key")
        
        # Chama process_job.py no venv
        result = subprocess.run(
            [sys.executable, "process_job.py", key],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("process_job.py stdout:\n", result.stdout)
        print("process_job.py stderr:\n", result.stderr)

        return jsonify({
            "status": "ok",
            "stdout": result.stdout,
            "stderr": result.stderr
        })

    except subprocess.CalledProcessError as e:
        print("Erro ao executar process_job.py")
        print("stdout:", e.stdout)
        print("stderr:", e.stderr)
        return jsonify({
            "status": "error",
            "stdout": e.stdout,
            "stderr": e.stderr
        }), 500
    except Exception as e:
        print("Erro inesperado:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)