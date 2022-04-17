#producer
import os
import argparse
import pandas as pd


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, default=-1 ,help="id producer")
    parser.add_argument("--d", type=str, default="", help="csv dataset")
    parser.add_argument("--s", type=float, default=1, help="time(s) to delay sending msg")
    parser.add_argument("--l", type=int, default=0, help="times of iterate dataset, 0 < for forever")
    parser.add_argument("--t", type=str, help="topic")
    parser.add_argument("--server", type=str, delfault="bigdis.duckdns.org", help="ip server")
    return parser.parse_args()


class Producer():
    def __init__(self, args):
        self.ds = pd.read_csv(args.d, head=True)
        self.id = args.id
        
    def run(self, delay_time, loop):
        i = 
        while loop >= 0:
            #sen ds

if __name__ == "__main__":
    args = get_args()
    assert args.id > 1, "id must > 0"
    assert os.path.exists(args.d), "dataset not exists"
    assert args.t > 0, "arg-t must > 0"
    
    producer = Producer(args)
    print("Kafka")
    producer.run(delay_time=args.s, loop=args.l)
    