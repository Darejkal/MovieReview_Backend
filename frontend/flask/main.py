from flask import Flask, render_template, request,redirect
from kafka import KafkaProducer, KafkaConsumer
import asyncio
import os
import json
import re
app = Flask(__name__)
KAFKA_ADDRESS=os.environ["REDPANDA_ADDR"]
data = ["Search something ..."]
# Kafka producer configuration
kafka_producer_conf = {
    'bootstrap_servers': KAFKA_ADDRESS,  
}

# Kafka consumer configuration
kafka_consumer_conf = {
    'bootstrap_servers': KAFKA_ADDRESS,  
    # 'group_id': 'web',
    'auto_offset_reset': 'earliest'
}

producer = KafkaProducer(**kafka_producer_conf)
consumer = KafkaConsumer('inference-output', **kafka_consumer_conf)  

async def kafka_consumer():
    while True:
        message = next(consumer)
        result = message.value.decode('utf-8')
        app.config['search_result'] = result

async def produce_search_query(key:str,query:str):
    # Produce the search query to Kafka asynchronously
    producer.send('inference-input', key=str(key).encode('utf-8'), value=re.sub(r'( |\n)+',' ',query).encode('utf-8'))
record_list={}
async def process_search_query(query:str):
    # Simulate processing the search query
    records=consumer.poll()
    for topic, msg in records.items():
        for m in msg:
            if(type(m.key)==str and m.key.decode("utf-8")==query):
                result= json.loads(m.value)
                assert type(result) == list
                return result
    # return json.loads(record.value().)
    pass
@app.route('/')
def index():
    return render_template('index.html', items=data)

@app.route('/search', methods=['GET','POST'])
async def search():
    if request.method == 'GET':
        return redirect('/')
    query = request.form['query']
    query=str(query)
    # Produce the search query to Kafka asynchronously
    await produce_search_query(query,query)

    # Process the search query locally while waiting for the Kafka result
    search_result_task = asyncio.create_task(process_search_query(query))
    kafka_result_task = asyncio.create_task(kafka_consumer())

    await asyncio.gather(search_result_task, kafka_result_task)

    # Get the result from Kafka or the local search processing
    result = app.config.get('search_result', '')

    return render_template('index.html', items=result, query=query, result=result)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(kafka_consumer())
    app.run(debug=True, use_reloader=False,port="5000",host="0.0.0.0")
