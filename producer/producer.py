#producer
import os
import json
import time
import argparse
import pandas as pd

from kafka import KafkaProducer

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, default=-1 ,help="id producer")
    parser.add_argument("--d", type=str, default="", help="csv dataset")
    parser.add_argument("--s", type=float, default=1, help="time(s) to delay sending msg")
    parser.add_argument("--l", type=float, default=0, help="times of iterate dataset, 0 < for forever")
    parser.add_argument("--t", type=str, help="topic")
    parser.add_argument("--ip", type=str, default="localhost:9092", help="ip server:port")
    return parser.parse_args()


class Producer():
    
    def __init__(self, args):
        self.ds = pd.read_csv(args.d)
        self.id = args.id
        self.topic = args.t
        self.kafka = KafkaProducer(bootstrap_servers=args.ip, value_serializer= lambda v: json.dumps(v).encode('utf-8'))
        
    def run(self, delay_time, loop):
        
        while loop >= 0:
            if loop > 0: loop -= 1 - 5e-1 
            #send ds
            for row in self.ds.iterrows():
                msg = row
                print(msg)
                exit()
                self.kafka.send(self.topic, msg)
                self.kafka.flush()
                time.sleep(delay_time)
			#end send


if __name__ == "__main__":
    args = get_args()
    assert args.id > 1, "id must > 0"
    assert args.ip != "", "ip is not null"
    assert os.path.exists(args.d), "dataset not exists"
    assert args.s > 0, "arg-s must > 0"
    
    producer = Producer(args)
    print("Kafka")
    producer.run(delay_time=args.s, loop=args.l)