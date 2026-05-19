# 关闭 Flask 调试模式，防止重复创建 Spark
import os
os.environ["FLASK_DEBUG"] = "0"
os.environ["PYSPARK_PIN_THREAD"] = "true"

from flask import Flask, jsonify, request
import json
import time

# 只导入工具类，不自动启动 Spark
from kafka_utils import KafkaUtils
from lake_utils import LakeUtils

app = Flask(__name__)
app.debug = False  # 强制关闭调试

# 工具类
kafka = KafkaUtils()
lake = LakeUtils()
lake.ensure_dirs()

# ------------------------------
# API 接口
# ------------------------------
@app.route("/api/v1/health", methods=["GET"])
def health():
    return jsonify({
        "status": "running",
        "kafka": "connected",
        "api": "active"
    }), 200

@app.route("/api/v1/sensors", methods=["GET"])
def get_sensors():
    return jsonify({
        "sensors": ["temperature", "humidity", "pressure"]
    }), 200

@app.route("/api/v1/sensors/<sensor_name>/latest", methods=["GET"])
def get_latest(sensor_name):
    return jsonify({
        "sensor": sensor_name,
        "value": 25.5,
        "anomaly": False,
        "status": "ok"
    }), 200

@app.route("/api/v1/anomalies", methods=["GET"])
def get_anomalies():
    return jsonify({
        "total": 3,
        "anomalies": [
            {"sensor": "temperature", "value": 46.0, "anomaly": True}
        ]
    }), 200

@app.route("/api/v1/readings", methods=["POST"])
def publish_reading():
    try:
        data = request.get_json()
        p = kafka.get_producer()
        p.send("sensor-events", key=data["sensor"], value=data)
        p.flush()
        return jsonify({"status": "published"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # 关闭自动重载
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)