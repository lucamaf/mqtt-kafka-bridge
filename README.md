# What this is

# How is it built

## ✅ **Program Structure & Logic Overview**

This Python program is a **graphical MQTT-to-Kafka bridge** built using:

* **[NiceGUI](https://nicegui.io/)**: For creating a modern, browser-based UI
* **[Paho MQTT](https://www.eclipse.org/paho/)**: To subscribe to MQTT topics
* **[Kafka-Python](https://kafka-python.readthedocs.io/)**: To publish messages to a Kafka topic
* **Asyncio**: To handle asynchronous operations safely

### 🔧 Main Components

#### 1. **`BridgeState` Class**

* A singleton-like object managing all runtime state: MQTT/Kafka client instances, UI state, config, and connection status.
* Reads default configuration from environment variables but is fully customizable through the GUI.

#### 2. **Logging**

* `log_message()` updates the UI log with timestamped messages.
* Fallbacks to console logging if UI isn’t yet ready.

#### 3. **Kafka Integration**

* `init_kafka_producer()`: Sets up a Kafka producer asynchronously.
* `send_to_kafka()`: Converts MQTT payloads to JSON or string and sends them to Kafka with success/error callbacks.
* Handles Kafka retries and errors gracefully.

#### 4. **MQTT Integration**

* MQTT client is configured using Paho callbacks:

  * `on_connect`, `on_disconnect`, `on_message`, `on_subscribe`
* On receiving a message via MQTT, the payload is sent to Kafka using the `send_to_kafka()` coroutine in a fresh event loop.
* Credentials and protocol version (v3.1.1 or v5) are configurable.

#### 5. **Bridge Control**

* `start_bridge()`: Starts the MQTT and Kafka clients using current UI configuration.
* `stop_bridge()`: Stops both clients cleanly and updates UI status.

#### 6. **UI with NiceGUI**

* Built using the `@ui.page('/')` route.
* Includes:

  * MQTT and Kafka configuration panels
  * Status labels
  * Start/Stop buttons
  * Live log viewer
* UI elements are dynamically bound to `bridge_state` for reactive updates.
* UI prevents changes while the bridge is running.

#### 7. **Lifecycle Management**

* Graceful shutdown is handled via `app.on_shutdown()` which stops the bridge if running.

---
# How to run it

## 🛠️ Installation & Setup Instructions

### 📋 **Prerequisites**

* Python 3.9+
* MQTT broker (e.g., Mosquitto) accessible
* Kafka broker running (e.g., Apache Kafka on `localhost:9092`)
* Optionally set environment variables to prefill configuration

### 🧪 **Install Required Packages**

Use a virtual environment and install with pip:

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

pip install nicegui paho-mqtt kafka-python
```

### 📁 **Optional: Environment Variables**

You can pre-configure the bridge via environment variables:

```bash
export MQTT_BROKER_HOST=localhost
export MQTT_BROKER_PORT=1883
export MQTT_TOPIC_SUBSCRIBE=telemetry/data
export MQTT_CLIENT_ID=my-mqtt-client
export MQTT_USERNAME=myuser
export MQTT_PASSWORD=mypass
export MQTT_VERSION=5

export KAFKA_BROKER_HOST=localhost:9092
export KAFKA_TOPIC_PUBLISH=mqtt-data
export KAFKA_CLIENT_ID=my-kafka-client
```

### 🚀 **Run the Program**

```bash
python bridge.py
```

Open the GUI in your browser at [http://localhost:8080](http://localhost:8080)

---

Let me know if you'd like this as a Markdown file or integrated into in-code documentation comments.

### **Run in Containers**

▶️ Build and Run the Docker Container

#### Build the container
docker build -t mqtt-kafka-bridge .

#### Run the container
docker run -p 8080:8080 --env-file .env mqtt-kafka-bridge

✅ Optional .env File Example

MQTT_BROKER_HOST=your-mqtt-broker
MQTT_BROKER_PORT=1883
MQTT_TOPIC_SUBSCRIBE=telemetry/data
MQTT_CLIENT_ID=my-client

KAFKA_BROKER_HOST=your-kafka-broker:9092
KAFKA_TOPIC_PUBLISH=mqtt-data
KAFKA_CLIENT_ID=my-kafka-client

This allows you to override defaults from outside the container.

