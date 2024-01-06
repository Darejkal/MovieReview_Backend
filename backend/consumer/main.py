from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer,datasets,losses
import os
from typing import List
import json
import torch
KAFKA_ADDRESS=os.environ['REDPANDA_ADDR']
SAMPLE_SIZE=300
consumer=KafkaConsumer(topics='review',bootstrap_servers=KAFKA_ADDRESS)
items=[]
try:
    model=SentenceTransformer("/model/latest.pth")
except:
    model = SentenceTransformer("all-MiniLM-L6-v2")
def parseBatch(batch:List):
    new_batch=[]
    for x in batch:
        try:
            new_x=json.load(x.get("value"))
            if new_x:
                new_batch.append((new_x,x.get("movie")))
        except:
            pass
    return new_batch
def onBatchSize(batch:List):
    print(parseBatch(batch))
    train_dataloader=datasets.NoDuplicatesDataLoader(parseBatch(batch),batch_size=8)
    train_loss = losses.MultipleNegativesRankingLoss(model)
    num_epochs = 3
    warmup_steps = int(len(train_dataloader) * num_epochs * 0.1)
    model.fit(train_objectives=[(train_dataloader, train_loss)], epochs=num_epochs, warmup_steps=warmup_steps, show_progress_bar=True)
    os.makedirs('/model', exist_ok=True)
    model.save('/model/lastest.pth')
while True:
    new_items=consumer.poll(timeout_ms=1000)["review"]
    if new_items:
        items.extend(new_items)
    if len(items)>SAMPLE_SIZE:
        onBatchSize(items[:SAMPLE_SIZE])
        items=items[SAMPLE_SIZE:]

