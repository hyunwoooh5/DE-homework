import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'rides'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-rides-console',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

try:
    count = 0
    for message in consumer:
        ride = message.value
        td = ride.trip_distance
        '''
        print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
            f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
            )
        '''

        if td>5.0:
            count += 1
except KeyboardInterrupt:
    print("stop the process")

print(count)    

consumer.close()