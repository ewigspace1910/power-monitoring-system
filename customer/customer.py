#producer
import os
import json
import time
import argparse
import pandas as pd

from schema import Resident_Schema


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s", type=int, default=1, help="iterval, time(s) to split streams")
    parser.add_argument("--t", type=str, help="topic")
    parser.add_argument("--ip", type=str, default="localhost:9092", help="ip server:port")
    return parser.parse_args()


class Customer():
    
    def __init__(self, args):

        
    def run(self, delay_time, loop):
        

			#end send


if __name__ == "__main__":
    args = get_args()
