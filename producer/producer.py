from data_generator import generateLoginRequest, generateRequest
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import threading
import uuid

global id
id = -1
threadLock = threading.Lock()
threadLock.acquire()
clientId = uuid.uuid4()

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

def producer_thread():
    while id == -1:
        # Generate a message
        request = generateLoginRequest(0, str(clientId))   
        # Send it to our 'messages' topic
        producer.send('messages', request)
        threadLock.acquire()
    
    request = generateRequest(id, str(clientId))
    while(request["transaction"]  != 4):
        producer.send('messages', request)
        threadLock.acquire()
        request = generateRequest(id, str(clientId))
    
    producer.send('messages', request)
    

def consumer_thread():
    # Create a temporary and unique topic for receiving replies
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic_list = []
    topic_list.append(NewTopic(str(clientId), 1, 1))
    admin_client.create_topics(topic_list)

    # Receive reply
    for message in consumer:
        msg = message.value.decode('utf-8')
        reply = json.loads(msg)
        if( 'id' in reply):
            global id
            id = reply["id"]
        elif( 'message' in reply):
            if(str(reply["message"]) == "Exit"): 
                print(reply["message"])
                threadLock.release()
                break
            else: print(reply["message"])
        threadLock.release()

    # Delete the temporary topic
    admin_client.delete_topics([clientId])    

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

# Kafka Consumer 
consumer = KafkaConsumer(
    str(clientId),
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)


if __name__ == '__main__':
    
    prod = threading.Thread(target = producer_thread, args=())
    prod.start()

    cons = threading.Thread(target=consumer_thread, args=())
    cons.start()

    prod.join()
    cons.join()


        

