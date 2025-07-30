import asyncio
import aio_pika
import json
import pika
rabbitmq_host = '192.168.1.30' 
rabbitmq_username = 'shubham'
rabbitmq_password = 'shubham'
credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)

rabbitmq_port = 5672  # Default AMQP port for RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=rabbitmq_host,
    port=rabbitmq_port,
    credentials=credentials,
    virtual_host='/' # Default virtual host, change if using a different one
))
channel = connection.channel()

# channel.queue_declare(queue="hello")

body = {
    "path": "sqlite-mitaly-gnucash.gnucash",
    "accountName":"Expenses",
    "startDate":"01-01-2025",
    "endDate":"30-06-2025",
    "job_id":"12345"
}
channel.basic_publish(exchange='',
routing_key='portfolio-request',
body=json.dumps(body))

print('Sent message')