%pip install azure-eventhub



import time
import asyncio
import os
import uuid
import datetime
import random
import json

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

CONNECTION_STR = 'Endpoint=sb://msft2eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NWRqoAsV6PXEaTWxIkEoO2Zturjzq8yxgAwaO4t9/aw='
EVENTHUB_NAME = 'sample-eventshub'

#while True: #while loop to repeatedly send messages to even hub
#  sleep(10) #to give 10s gap between messages
  
async def run():

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENTHUB_NAME
    )

    to_send_message_cnt = 50
    #bytes_per_message = 256
    
    devices = []
    for x in range(0, 10):
        devices.append(str(uuid.uuid4()))


    async with producer:
        event_data_batch = await producer.create_batch()
        for i in range(to_send_message_cnt):
          for dev in devices:
              reading = {'source': 'wind-turbine-geo-sensor', 
                         'id': 'tst_sensor', 
                         'timestamp': str(datetime.datetime.utcnow()),
                         'device_id': dev,
                         'Export_Unit': random.random(),
                         'battery_current': random.randint(10, 20), 
                         'solar_KWH': random.randint(10, 20),
                         'Grid_VOLTAGE': random.randint(250, 800),
                         'turbine_RPM': random.randint(400, 500)}
              s = json.dumps(reading)
              print(i)
              print(dev)
              print(s)
          event_data = EventData(s)
          try:
              event_data_batch.add(event_data)
          except ValueError:
              await producer.send_batch(event_data_batch)
              event_data_batch = await producer.create_batch()
              event_data_batch.add(event_data)
          if len(event_data_batch) > 0:
              await producer.send_batch(event_data_batch)
          
          print("Send messages in {} seconds.".format(time.time() - start_time))

loop = asyncio.get_event_loop()
start_time = time.time()
loop.run_until_complete(run())

