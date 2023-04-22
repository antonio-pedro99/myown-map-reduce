# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("..")
from functions.reduce import reduce
from pathlib import Path
from proto.map_reduce_pb2_grpc import MasterServicer, add_MasterServicer_to_server, MasterStub
from proto.map_reduce_pb2 import Response, Notification
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader

def get_input():
    # get input from input files
    pass


def shuffle():
    # shuffle the input
    pass

def sort():
    #sort the input
    pass

#RPC call
def StartReducer():
    pass


if __name__=='__main__':
    reduce()