import requests
import io
import avro.schema
import avro.io
from confluent_kafka import Producer
import json

# -------- Config --------
schema_registry_url = "http://10.0.0.0:8081"
bootstrap_servers = "ur_bootstrap_servers:9092,ur_bootstrap_servers:9092,ur_bootstrap_servers:9092"
topic = "test"
subject = f"{topic}-value"

# -------- Avro Schema --------
schema_str = """
{
  "type": "record",
  "name": "Test",
  "fields": [
    {"name": "f1", "type": "string"}
  ]
}
"""
schema = avro.schema.parse(schema_str)

# -------- Register Schema --------
register_url = f"{schema_registry_url}/subjects/{subject}/versions"
headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
payload = json.dumps({"schema": schema_str})
response = requests.post(register_url, headers=headers, data=payload)

if response.status_code == 200:
    schema_id = response.json()['id']
    print(f"Schema registered with ID: {schema_id}")
else:
    raise Exception(f"Schema registration failed: {response.text}")

# -------- Serialize Avro Message --------
def avro_encode(schema_id, data):
    bytes_writer = io.BytesIO()
    # Confluent wire format magic byte + schema ID (int32 big endian)
    bytes_writer.write(b'\x00')  # magic byte
    bytes_writer.write(schema_id.to_bytes(4, byteorder='big'))

    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(data, encoder)

    return bytes_writer.getvalue()

# -------- Kafka Producer --------
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

# Example data
record = {"f1": "hello world"}

# Serialize and send
encoded_value = avro_encode(schema_id, record)

producer.produce(topic=topic, value=encoded_value)
producer.flush()

print("Message sent!")
