from flask import Flask, request, jsonify
from lake_utils import LakeUtils
from kafka_utils import KafkaUtils
from datetime import datetime

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

lake = LakeUtils()
kafka = KafkaUtils()


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": "Invalid request parameters"}), 400

@app.errorhandler(500)
def server_error(e):
    import traceback
    traceback.print_exc()
    return jsonify({"error": "Internal server error"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "spark_connected": True,
        "kafka_connected": True
    }), 200


@app.route("/api/sensors", methods=["GET"])
def get_sensors():
    sensors = lake.get_sensor_list()
    return jsonify({"count": len(sensors), "sensor_types": sensors}), 200


@app.route("/api/latest/<sensor_type>", methods=["GET"])
def get_latest_reading(sensor_type):
    result = lake.get_latest_reading(sensor_type)
    if not result:
        return jsonify({"error": f"No data found for sensor type: {sensor_type}"}), 404
    return jsonify({
        "sensor_type": result["sensor_type"],
        "value": result["value"],
        "unit": result["unit"],
        "timestamp": result["timestamp"],
        "source": result["source"],
        "is_anomaly": bool(result["is_anomaly"])
    }), 200


@app.route("/api/stats/<sensor_type>", methods=["GET"])
def get_sensor_stats(sensor_type):
    result = lake.get_sensor_stats(sensor_type)
    if not result:
        return jsonify({"error": f"No data found for sensor type: {sensor_type}"}), 404
    result["anomaly_rate"] = round(result["anomaly_count"] / result["total_records"], 4)
    return jsonify(result), 200


@app.route("/api/anomalies", methods=["GET"])
def get_anomalies():
    sensor_type = request.args.get("sensor_type")
    limit = request.args.get("limit", default=10, type=int)
    anomalies = lake.get_anomalies(sensor_type, limit)
    return jsonify({"count": len(anomalies), "anomalies": anomalies}), 200


@app.route("/api/readings", methods=["POST"])
def post_reading():
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    required_fields = ["sensor_type", "value", "unit", "timestamp", "source"]
    for field in required_fields:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400


    if not isinstance(data["sensor_type"], str):
        return jsonify({"error": "sensor_type must be a string"}), 400
    if not isinstance(data["value"], (int, float)):
        return jsonify({"error": "value must be a number"}), 400
    if not isinstance(data["unit"], str):
        return jsonify({"error": "unit must be a string"}), 400
    if not isinstance(data["timestamp"], (int, float)):
        return jsonify({"error": "timestamp must be a number"}), 400
    if not isinstance(data["source"], str):
        return jsonify({"error": "source must be a string"}), 400

    kafka.send_reading(data)
    return jsonify({
        "status": "success",
        "message": "Reading sent to Kafka",
        "data": data
    }), 201

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)