from confluent_kafka import Consumer
import requests
import io
import avro.schema
import avro.io
import struct

# -------- Config --------
schema_registry_url = "http://10.0.0.0:8081"
bootstrap_servers = "ur_bootstrap_servers:9092,ur_bootstrap_servers:9092,ur_bootstrap_servers:9092"
topic = "test"
group_id = "python-consumer-group"

# -------- Schema Cache --------
_schema_cache = {}

def get_schema(schema_id):
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    url = f"{schema_registry_url}/schemas/ids/{schema_id}"
    response = requests.get(url)
    response.raise_for_status()
    schema_str = response.json()["schema"]
    schema = avro.schema.parse(schema_str)
    _schema_cache[schema_id] = schema
    return schema

def decode_avro(msg_value):
    if len(msg_value) < 5 or msg_value[0] != 0:
        raise ValueError("Invalid message format")

    # Extract schema ID (4 bytes after magic byte)
    schema_id = struct.unpack(">I", msg_value[1:5])[0]
    schema = get_schema(schema_id)

    # Deserialize Avro
    bytes_reader = io.BytesIO(msg_value[5:])
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

# -------- Kafka Consumer --------
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])

print("Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        decoded_value = decode_avro(msg.value())
        print(f"Received: {decoded_value}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
