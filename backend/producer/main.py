from kafka import KafkaProducer
from flask import Flask,request
import json
from typing import Dict,Callable
import os
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
producer=KafkaProducer(bootstrap_servers=KAFKA_ADDRESS,acks="all")
app = Flask(__name__)
print("All set")

def try_n_times(func:Callable,kwargs:Dict,onTimeout:Callable=None,n:int=3):
    while(n>0):
        try:
            result=func(**kwargs)
            return result
        except:
            n-=1
            if onTimeout:
                onTimeout()
    return None
def check_review(data:Dict):
    return all(field in data for field in ["critic","movie","state","date"])
@app.route("/test",methods=["GET"])
def test_route():
    return {"status":True}
@app.route("/review/push",methods=["POST"])
def review_push():
    data=json.loads(request.data)
    if data:
        future=try_n_times(producer.send,{"topic":'review', "value":request.data})
    else:
        return {"status":False,"msg":"Data format error"}
    if future is None:
        return {"status":False}
    else:
        return {"status":True}
if __name__ == '__main__':
   app.run(debug=False, host='0.0.0.0')