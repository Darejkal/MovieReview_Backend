from kafka import KafkaConsumer
import psycopg2
import os
import json
from typing import Dict,List
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
SAMPLE_SIZE=1
# we use a context manager to scope the cursor session
def parseItem(item:bytes):
    obj:Dict=json.loads(item)
    return(obj.get("critic",None),obj.get("publication",None),obj.get("state",None),obj.get("review",None),obj.get("date",None),obj.get("grade",None),obj.get("movie",None))
items=[]
consumer=KafkaConsumer('review',bootstrap_servers=KAFKA_ADDRESS)
def onBatchSize(curs,batch:List[Dict]):
    parsed_batch=[]
    for item in batch:
        parsed_batch.append((
            item.get('critic',None),
            item.get('publication',None),
            item.get('state',None),
            item.get('review',None),
            item.get('date',None),
            item.get('grade',None),
            item.get('movie',None),
        ))
    query="INSERT INTO review_data (critic,publication,state,review,date,grade,movie) VALUES %s;"
    try:
        curs.execute(query,parsed_batch)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error",error)
conn=None
while True:
    try:
        conn = psycopg2.connect(
            dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],host="postgres",port="5432"
        )
        print(conn.info)
        break
    except:
        print("Unable to connect to the database")
with conn.cursor() as curs:
    curs.execute("""
        CREATE TABLE IF NOT EXISTS review_data (
            id serial primary key,
            critic text,
            publication text,
            state text,
            review text,
            date text,
            grade text,
            movie text  
        );
    """)
    conn.commit()
    while True:
        new_items=consumer.poll(timeout_ms=1000)
        for _,v in new_items.items():
            try:
                items.extend([json.loads(item.value) for item in v])
            except:
                pass
        if len(items)>SAMPLE_SIZE:
            onBatchSize(curs,items[:SAMPLE_SIZE])
            conn.commit()
            print("New batch commited")
            items=items[SAMPLE_SIZE:] 

