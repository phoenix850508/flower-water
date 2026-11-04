from rabbitmq.publisher import channel, connection

def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()} mV")

channel.queue_declare(queue='moisture', durable=True,
                      arguments={'x-queue-type': 'quorum'})
channel.basic_consume(queue='moisture', on_message_callback=callback, auto_ack=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
finally:
    connection.close()