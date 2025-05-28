import os
import time
import json
import asyncio
# import threading # No longer explicitly needed here, Paho manages its own thread.
from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
from kafka.errors import KafkaError
from nicegui import ui, app

# --- Application State ---
class BridgeState:
    def __init__(self):
        # Connection and Operational State
        self.is_running = False
        self.mqtt_client: mqtt_client.Client | None = None
        self.kafka_producer: KafkaProducer | None = None
        self.mqtt_status = "MQTT: Disconnected"
        self.kafka_status = "Kafka: Not Initialized"
        self.log_area: ui.log | None = None

        # Configuration - Initialized from environment variables as defaults
        self.mqtt_broker_host = os.environ.get("MQTT_BROKER_HOST", "localhost")
        self.mqtt_broker_port = int(os.environ.get("MQTT_BROKER_PORT", 1883))
        self.mqtt_topic_subscribe = os.environ.get("MQTT_TOPIC_SUBSCRIBE", "telemetry/data")
        self.mqtt_client_id = os.environ.get("MQTT_CLIENT_ID", f"mqtt-kafka-bridge-nicegui-{os.getpid()}")
        self.mqtt_username = os.environ.get("MQTT_USERNAME", "")
        self.mqtt_password = os.environ.get("MQTT_PASSWORD", "")
        # Default MQTT protocol version string for ui.select
        self.mqtt_protocol_str = "MQTTv5" if os.environ.get("MQTT_VERSION", "5") == "5" else "MQTTv3.1.1"

        self.kafka_broker_host = os.environ.get("KAFKA_BROKER_HOST", "localhost:9092")
        self.kafka_topic_publish = os.environ.get("KAFKA_TOPIC_PUBLISH", "mqtt-data")
        self.kafka_client_id = os.environ.get("KAFKA_CLIENT_ID", f"mqtt-kafka-bridge-nicegui-producer-{os.getpid()}")

    def get_mqtt_protocol_version(self):
        """Converts protocol string from UI to Paho MQTT constant."""
        return mqtt_client.MQTTv5 if self.mqtt_protocol_str == "MQTTv5" else mqtt_client.MQTTv311

    def reset_for_stop(self):
        """Resets status messages when the bridge is stopped. Config values are preserved."""
        self.mqtt_status = "MQTT: Disconnected"
        self.kafka_status = "Kafka: Closed"
        # Note: self.is_running is set to False by the stop_bridge function itself.

# Create a global state object
bridge_state = BridgeState()

# --- Helper for running blocking IO in thread ---
#async def run_in_executor(func, *args):
#    """Runs blocking functions in a separate thread to avoid blocking asyncio loop."""
#    loop = asyncio.get_running_loop()
#    return await loop.run_in_executor(None, func, *args)

# --- Logging ---
def log_message(message: str):
    """Pushes a timestamped message to the UI log area."""
    if bridge_state.log_area:
        timestamp = time.strftime('%H:%M:%S')
        bridge_state.log_area.push(f"{timestamp} - {message}")
    else:
        print(f"LOG (No UI): {message}") # Fallback if UI log element isn't ready

# --- Kafka Functions ---
async def init_kafka_producer() -> bool:
    loop = asyncio.get_running_loop()
    """Initializes the Kafka producer using configuration from bridge_state."""
    bridge_state.kafka_status = "Kafka: Attempting to connect..."
    log_message(f"Attempting to connect to Kafka broker at {bridge_state.kafka_broker_host}...")
    try:
        bridge_state.kafka_producer = await asyncio.to_thread(
            #None,
            KafkaProducer,
            bootstrap_servers=bridge_state.kafka_broker_host, # Use state value
            client_id=bridge_state.kafka_client_id,             # Use state value
            value_serializer=lambda v: json.dumps(v).encode('utf-8') if isinstance(v, dict) else str(v).encode('utf-8'),
            retries=3
            # Add SASL/SSL configurations here if needed, potentially from more UI fields
        )
        bridge_state.kafka_status = f"Kafka: Connected to {bridge_state.kafka_broker_host}"
        log_message(f"Successfully connected to Kafka broker: {bridge_state.kafka_broker_host}")
        return True
    except KafkaError as e:
        log_message(f"Failed to connect to Kafka: {e}")
        bridge_state.kafka_status = f"Kafka: Connection Failed - {e}"
        bridge_state.kafka_producer = None
        return False
    except Exception as e:
        log_message(f"An unexpected error occurred while connecting to Kafka: {e}")
        bridge_state.kafka_status = f"Kafka: Error - {e}"
        bridge_state.kafka_producer = None
        return False

def kafka_send_success_callback(record_metadata, original_payload):
    """Callback for successful Kafka message send."""
    log_message(f"Kafka: Sent OK (Topic: {record_metadata.topic}, Offset: {record_metadata.offset}). Msg: {original_payload!r}")

def kafka_send_error_callback(exception, original_payload):
    """Callback for failed Kafka message send."""
    log_message(f"Kafka: Send FAILED. Error: {exception}. Msg: {original_payload!r}")
    bridge_state.kafka_status = f"Kafka: Send Error - {exception}"
    if bridge_state.kafka_producer:
        asyncio.create_task(asyncio.to_thread(bridge_state.kafka_producer.close)) # Close potentially broken producer
    bridge_state.kafka_producer = None # Mark as None to trigger re-init attempt on next send

async def send_to_kafka(message_payload):
    """Sends a message to Kafka using topic from bridge_state."""
    if not bridge_state.kafka_producer:
        log_message("Error: Kafka producer not available. Attempting to reinitialize...")
        bridge_state.kafka_status = "Kafka: Producer unavailable, re-initializing..."
        if not await init_kafka_producer(): # This will use current config from bridge_state
            log_message("Failed to reinitialize Kafka producer. Message not sent.")
            return
        if not bridge_state.kafka_producer:
            log_message("Kafka producer still not available after re-init. Message not sent.")
            return

    original_payload_for_log = message_payload
    processed_payload = original_payload_for_log # Default to original

    try:
        if isinstance(original_payload_for_log, bytes):
            try:
                decoded_payload = original_payload_for_log.decode('utf-8')
                # Attempt to parse if it looks like JSON
                if decoded_payload.strip().startswith('{') and decoded_payload.strip().endswith('}'):
                    processed_payload = json.loads(decoded_payload)
                else:
                    processed_payload = decoded_payload # Send as plain string
            except UnicodeDecodeError:
                log_message(f"Warning: MQTT payload not UTF-8. Original: {original_payload_for_log!r}")
                processed_payload = str(original_payload_for_log) # Send as string representation of bytes
            except json.JSONDecodeError:
                log_message(f"Warning: MQTT payload looked like JSON but failed to parse. Original: {original_payload_for_log!r}")
                processed_payload = decoded_payload # Send as plain string (already decoded)
        
        # Ensure payload is serializable by KafkaProducer's value_serializer
        # Our serializer handles dicts and strings. If it's already a dict or string, it's fine.
        # If it's bytes that couldn't be decoded, it's now a string representation of bytes.

        log_message(f"Sending to Kafka topic '{bridge_state.kafka_topic_publish}': {processed_payload}")
        
        future = bridge_state.kafka_producer.send(bridge_state.kafka_topic_publish, value=processed_payload)
        future.add_callback(kafka_send_success_callback, original_payload_for_log)
        future.add_errback(kafka_send_error_callback, original_payload_for_log)

    except KafkaError as e: # Should be caught by errback, but as a fallback
        log_message(f"Kafka send error (direct): {e}. Payload: {original_payload_for_log!r}")
        bridge_state.kafka_status = f"Kafka: Send Error - {e}"
        if bridge_state.kafka_producer:
            await asyncio.to_thread(bridge_state.kafka_producer.close)
        bridge_state.kafka_producer = None
    except Exception as e:
        log_message(f"Unexpected error sending to Kafka: {e}. Payload: {original_payload_for_log!r}")


# --- MQTT Callback Functions ---
# These run in Paho's thread. UI updates should be thread-safe.
# NiceGUI's state updates and log.push are generally thread-safe.

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback for MQTT connection."""
    if rc == 0:
        protocol_version_str = "v5" if client.protocol == mqtt_client.MQTTv5 else "v3.1.1"
        status_msg = f"MQTT: Connected to {bridge_state.mqtt_broker_host}:{bridge_state.mqtt_broker_port} ({protocol_version_str})"
        bridge_state.mqtt_status = status_msg
        log_message(status_msg)
        # Subscribe using topic from bridge_state
        client.subscribe(bridge_state.mqtt_topic_subscribe)
    else:
        err_msg = f"MQTT: Connection Failed (rc: {rc})"
        bridge_state.mqtt_status = err_msg
        log_message(err_msg)
        # Add more specific error messages if needed
        if rc == 1: log_message("MQTT Error Detail: Connection refused - incorrect protocol version")
        elif rc == 2: log_message("MQTT Error Detail: Connection refused - invalid client identifier")
        elif rc == 3: log_message("MQTT Error Detail: Connection refused - server unavailable")
        elif rc == 4: log_message("MQTT Error Detail: Connection refused - bad username or password")
        elif rc == 5: log_message("MQTT Error Detail: Connection refused - not authorised")
        
        bridge_state.is_running = False # Important: Stop bridging if initial MQTT connection fails

def on_disconnect(client, userdata, rc, properties=None):
    """Callback for MQTT disconnection."""
    status_msg = f"MQTT: Disconnected (rc: {rc})"
    bridge_state.mqtt_status = status_msg
    log_message(status_msg)
    if bridge_state.is_running: # If not a deliberate stop
        log_message("MQTT: Will attempt to reconnect if broker is available (Paho's auto-reconnect)...")
    else: # Deliberate stop
        log_message("MQTT: Disconnected as part of controlled shutdown.")


def on_message(client, userdata, msg):
    """Callback for received MQTT message."""
    log_message(f"MQTT RX: Topic='{msg.topic}', QoS={msg.qos}, Payload='{msg.payload!r}'")
    if bridge_state.is_running and bridge_state.kafka_producer:
        # Schedule send_to_kafka to run in the main asyncio loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop) # Set the new loop for this thread
        log_message(f"Forwarding message to Kafka topic '{bridge_state.kafka_topic_publish}'...")
        loop.run_until_complete(
            send_to_kafka(msg.payload) # Pass only payload
        )
        loop.close() # Close the loop after use
        #asyncio.run_coroutine_threadsafe(
        #    send_to_kafka(msg.payload), # Pass only payload
        #    asyncio.get_running_loop()
        #)
        #asyncio.to_thread(
        #    send_to_kafka(msg.payload), # Pass only payload
        #)
    elif not bridge_state.kafka_producer and bridge_state.is_running:
        log_message("Kafka producer not ready, message not forwarded.")
    elif not bridge_state.is_running:
        log_message("Bridge is stopped, message not forwarded.")


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    """Callback for MQTT subscription."""
    log_message(f"MQTT: Subscribed to '{bridge_state.mqtt_topic_subscribe}' (Granted QoS: {granted_qos})")

# --- Bridge Control Functions ---
async def start_bridge():
    """Starts the MQTT to Kafka bridge using current UI configurations."""
    if bridge_state.is_running:
        log_message("Bridge is already running.")
        return

    log_message("Starting bridge with current configuration...")
    bridge_state.is_running = True # UI elements will react (e.g., disable config inputs)

    # Initialize Kafka Producer first
    if not await init_kafka_producer(): # Uses config from bridge_state
        log_message("Kafka producer initialization failed. Bridge not started.")
        bridge_state.is_running = False
        bridge_state.reset_for_stop() # Reset status, config remains
        return

    # Initialize MQTT Client
    # Get protocol from bridge_state (converted from string)
    protocol = bridge_state.get_mqtt_protocol_version()
    bridge_state.mqtt_client = mqtt_client.Client(client_id=bridge_state.mqtt_client_id, protocol=protocol)
    
    bridge_state.mqtt_client.on_connect = on_connect
    bridge_state.mqtt_client.on_message = on_message
    bridge_state.mqtt_client.on_subscribe = on_subscribe
    bridge_state.mqtt_client.on_disconnect = on_disconnect

    # Set username and password if provided in bridge_state
    if bridge_state.mqtt_username and bridge_state.mqtt_password:
        bridge_state.mqtt_client.username_pw_set(bridge_state.mqtt_username, bridge_state.mqtt_password)
    elif bridge_state.mqtt_username: # Username only
         bridge_state.mqtt_client.username_pw_set(bridge_state.mqtt_username)


    try:
        bridge_state.mqtt_status = f"MQTT: Connecting to {bridge_state.mqtt_broker_host}..."
        log_message(f"MQTT: Connecting to {bridge_state.mqtt_broker_host}...")

        
        # Paho's connect is blocking, run in executor
        #bridge_state.mqtt_client.connect_async(
        #    bridge_state.mqtt_broker_host,
        #    bridge_state.mqtt_broker_port,
        #    60 # keepalive
        #)
        await asyncio.to_thread(
            bridge_state.mqtt_client.connect,
            bridge_state.mqtt_broker_host,
            bridge_state.mqtt_broker_port,
            60 # keepalive
        )
        # on_connect callback will handle status update and subscription
    except Exception as e:
        log_message(f"MQTT connection error during connect call: {e}")
        bridge_state.mqtt_status = f"MQTT: Connection Error - {e}"
        bridge_state.is_running = False # Critical failure
        bridge_state.reset_for_stop()
        if bridge_state.kafka_producer: # Clean up Kafka if MQTT fails to connect
            await asyncio.to_thread(bridge_state.kafka_producer.close)
            bridge_state.kafka_producer = None
            bridge_state.kafka_status = "Kafka: Closed (MQTT connect failed)"
        return

    # Paho's loop_start() starts its own thread for network I/O.
    # This is non-blocking.
    bridge_state.mqtt_client.loop_start()
    log_message("MQTT client loop started.")
    # Note: is_running might be set to False in on_connect if connection fails,
    # so the UI should reflect that reactively.

async def stop_bridge():
    """Stops the MQTT to Kafka bridge."""
    if not bridge_state.is_running and not bridge_state.mqtt_client and not bridge_state.kafka_producer:
        log_message("Bridge is not running or clients are already cleared.")
        # Ensure is_running is false and UI reflects this
        bridge_state.is_running = False
        bridge_state.reset_for_stop()
        return

    log_message("Attempting to stop bridge...")
    bridge_state.is_running = False # Signal to stop processing and enable UI config

    if bridge_state.mqtt_client:
        log_message("Stopping MQTT client loop and disconnecting...")
        try:
            await asyncio.to_thread(bridge_state.mqtt_client.loop_stop) # Stop network loop
            await asyncio.to_thread(bridge_state.mqtt_client.disconnect) # Disconnect
            log_message("MQTT client disconnected.")
        except Exception as e:
            log_message(f"Error stopping MQTT client: {e}")
        finally:
            bridge_state.mqtt_client = None # Clear instance

    if bridge_state.kafka_producer:
        log_message("Flushing and closing Kafka producer...")
        try:
            # await run_in_executor(bridge_state.kafka_producer.flush, timeout=5) # Optional explicit flush
            await asyncio.to_thread(bridge_state.kafka_producer.close, timeout=5) # Close producer
            log_message("Kafka producer closed.")
        except Exception as e:
            log_message(f"Error closing Kafka producer: {e}")
        finally:
            bridge_state.kafka_producer = None # Clear instance
    
    bridge_state.reset_for_stop() # Update status labels
    log_message("Bridge stopped.")


# --- NiceGUI UI Definition ---
@ui.page('/')
async def main_page():
    ui.label("MQTT to Kafka Bridge").classes('text-2xl font-semibold mb-4 text-center')

    # --- Configuration Section ---
    with ui.row().classes('w-full mb-4 no-wrap justify-around'):
        # MQTT Configuration Card
        with ui.card().classes('w-5/12'): # Adjust width as needed
            ui.label("MQTT Configuration").classes('text-lg font-medium')
            ui.separator()
            ui.input(label='Broker Host', placeholder='e.g., localhost') \
                .bind_value(bridge_state, 'mqtt_broker_host') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.number(label='Broker Port', placeholder='e.g., 1883', format='%.0f') \
                .bind_value(bridge_state, 'mqtt_broker_port') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Client ID', placeholder='Unique MQTT client ID') \
                .bind_value(bridge_state, 'mqtt_client_id') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Topic to Subscribe', placeholder='e.g., telemetry/data/#') \
                .bind_value(bridge_state, 'mqtt_topic_subscribe') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.select(['MQTTv5', 'MQTTv3.1.1'], label='Protocol Version') \
                .bind_value(bridge_state, 'mqtt_protocol_str') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Username (Optional)') \
                .bind_value(bridge_state, 'mqtt_username') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Password (Optional)', password=True, password_toggle_button=True) \
                .bind_value(bridge_state, 'mqtt_password') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)

        # Kafka Configuration Card
        with ui.card().classes('w-5/12'): # Adjust width as needed
            ui.label("Kafka Configuration").classes('text-lg font-medium')
            ui.separator()
            ui.input(label='Broker Host(s)', placeholder='e.g., kafka:9092 or host1:9092,host2:9092') \
                .bind_value(bridge_state, 'kafka_broker_host') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Client ID', placeholder='Unique Kafka client ID') \
                .bind_value(bridge_state, 'kafka_client_id') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
            ui.input(label='Topic to Publish', placeholder='e.g., mqtt-messages') \
                .bind_value(bridge_state, 'kafka_topic_publish') \
                .props('outlined dense') \
                .bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
    
    # --- Status Display ---
    with ui.card().classes('w-full mb-4'):
        ui.label("Status").classes('text-lg font-medium')
        ui.separator()
        with ui.row():
            ui.label().bind_text_from(bridge_state, 'mqtt_status').classes('mr-4')
            ui.label().bind_text_from(bridge_state, 'kafka_status')

    # --- Control Buttons ---
    with ui.row().classes('mb-4 w-full justify-center'):
        start_button = ui.button("Start Bridge", on_click=start_bridge).props('color=positive icon=play_arrow push')
        stop_button = ui.button("Stop Bridge", on_click=stop_bridge).props('color=negative icon=stop push')
        
        # Enable/disable buttons based on running state
        start_button.bind_enabled_from(bridge_state, 'is_running', backward=lambda x: not x)
        stop_button.bind_enabled_from(bridge_state, 'is_running')

    # --- Log Area ---
    ui.label("Logs").classes('text-lg font-medium mt-4 text-center')
    bridge_state.log_area = ui.log(max_lines=500).classes('w-full h-64 border rounded p-2 bg-gray-100 shadow-inner')
    
    log_message("Bridge UI initialized. Configure and press 'Start Bridge'.")

# --- Application Lifecycle Hooks ---
async def on_shutdown():
    """Cleanup tasks when the NiceGUI application is shutting down."""
    log_message("Application shutting down...")
    if bridge_state.is_running:
        await stop_bridge() # Ensure graceful shutdown of the bridge

app.on_shutdown(on_shutdown) # Register the shutdown handler

# --- Run the NiceGUI app ---
# reload=False is generally better for applications with background tasks or state
ui.run(title="MQTT-Kafka Bridge Configurable", reload=False, uvicorn_reload_dirs=None, uvicorn_reload_includes=None)