from gpiozero import MCP3008
import logging
import time
from rabbitmq.publisher import channel, connection

QUEUE = 'moisture'

logging.basicConfig(
    filename='moisture_sensor.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

channel.queue_declare(queue=QUEUE, durable=True,
                      arguments={'x-queue-type': 'quorum'})


moisture = MCP3008(0)


while True:  
    time.sleep(10)
    channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=(3300 * moisture.value))
    logging.info(f"Moisture: {3300 * moisture.value:.3f} mV")