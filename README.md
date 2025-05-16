# Confluent schema registry integration with Amazon MSK

This project demonstrates how to:
- Deploy **Confluent Schema Registry** on **Amazon ECS Fargate**
- Integrate it with **Amazon MSK** (Plaintext authentication)
- Produce and consume **Avro-encoded** Kafka messages using **Python**

### 🔧 What's Included
- ✅ CloudFormation template to deploy Schema Registry behind a public IP
- ✅ Python producer that registers schemas and sends Avro messages to a Kafka topic
- ✅ Python consumer that fetches schemas from the registry and decodes messages
- ✅ Minimal setup using Fargate

### 🧪 Technologies
- AWS Fargate for Amazon ECS
- Amazon MSK
- Confluent Schema Registry (open source Docker image)
- Python

### 🚀 Quick Start
1. Deploy the CloudFormation stack
2. Update the producer and consumer scripts with your MSK cluster bootstrap url and Schema Registry endpoints
3. Send and read Avro messages
4. Make sure the subnet you are choosing has internet access to download the Docker image
5. Update the BOOTSTRAPSERVERS url before deploying the CloudFormation template


