from flask import Flask, request, jsonify
import os

app = Flask(__name__)

INGEST_API_KEY = os.getenv("INGEST_API_KEY")

@app.route("/")
def home():
    return "Backend is running"

@app.route("/ingest", methods=["POST"])
def ingest():
    auth = request.headers.get("Authorization")

    if auth != f"Bearer {INGEST_API_KEY}":
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    return jsonify({
        "message": "Data received successfully",
        "data": data
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
