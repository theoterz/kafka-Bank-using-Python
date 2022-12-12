import json 
from kafka import KafkaConsumer, KafkaProducer
from database import Database
from data_generator import loginReply, Reply

# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )

    # Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=serializer
    )

    db = Database()

    for message in consumer:
        msg = message.value.decode('utf-8')
        request = json.loads(msg)
        clientId = request["uuid"]
        if(request["transaction"] == 0):
            username = request["username"]
            password = request["password"]
            id = db.login(username, password)
            reply = loginReply(id)
        else:
            id = request["id"]
            ammount = request["ammount"]
            if(request["transaction"] == 1):
                result = db.withdrawal(id, ammount)
                reply = Reply(result)
            elif(request["transaction"] == 2):
                result = db.deposit(id, ammount)
                reply = Reply(result)
            elif(request["transaction"] == 3):
                result = db.showBalance(id)
                reply = Reply(result)
            elif(request["transaction"] == 4):
                result = "Exit"
                reply = Reply(result)
    
        producer.send(clientId, reply)
