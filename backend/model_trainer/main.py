from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer,datasets,losses,InputExample
import os
from typing import List,Dict
import json
import pandas as pd
import logging
import sys
root_logger= logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root_logger.addHandler(handler)
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
try:
    SAMPLE_SIZE=int(os.environ.get('MODEL_TRAINER_SAMPLE_SIZE',"300"))
except: SAMPLE_SIZE=300
root_logger.info(f"SAMPLE_SIZE {SAMPLE_SIZE}")
consumer=KafkaConsumer('review',bootstrap_servers=KAFKA_ADDRESS)
items=[]
try:
    model=SentenceTransformer("/model/latest.pth")
except:
    model = SentenceTransformer("all-MiniLM-L6-v2")
train_loss = losses.MultipleNegativesRankingLoss(model)
def onBatchSize(batch:List[Dict]):
    os.makedirs('/model', exist_ok=True)
    print("Started training")
    parsed_batch=[]
    for item in batch:
        print(item)
        if item.get("review",None) and item.get("movie",None):
            parsed_batch.append(InputExample(texts=(item["review"],item["movie"])))
    train_dataloader=datasets.NoDuplicatesDataLoader(parsed_batch,batch_size=8)
    num_epochs = 3
    warmup_steps = int(len(train_dataloader) * num_epochs * 0.1)
    model.fit(train_objectives=[(train_dataloader, train_loss)], epochs=num_epochs, warmup_steps=warmup_steps,checkpoint_path="/model/lastest.pth", show_progress_bar=True)
    print("New weights saved")
while True:
    new_items=consumer.poll(timeout_ms=1000)
    for _,v in new_items.items():
        try:
            items.extend([json.loads(item.value) for item in v])
        except:
            pass
    if len(items)>SAMPLE_SIZE:
        onBatchSize(items[:SAMPLE_SIZE])
        items=items[SAMPLE_SIZE:]

