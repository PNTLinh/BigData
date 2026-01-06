#!/usr/bin/env python3
"""
Simple Flask application for K8s deployment examples
"""
from flask import Flask, jsonify
import os
import socket

app = Flask(__name__)

@app.route('/')
def hello():
    hostname = socket.gethostname()
    return jsonify({
        'message': 'Hello from Kubernetes!',
        'hostname': hostname,
        'version': os.getenv('APP_VERSION', '1.0.0')
    })

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'}), 200

@app.route('/config')
def config():
    return jsonify({
        'database_host': os.getenv('DATABASE_HOST', 'not-set'),
        'api_key': '***' if os.getenv('API_KEY') else 'not-set'
    })

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

