# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
from pathlib import Path
from functions.map import map
from proto.map_reduce_pb2_grpc import MasterServicer, add_MasterServicer_to_server, MasterStub
from proto.map_reduce_pb2 import Response, Notification
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader
def get_input():
    # get input from input files
    pass

#RPC call
def StartMapper():
    pass


if __name__=='__main__':
    input_file = os.path.join("user_input", "ii", "Input1.txt")

    reader = RecordReader(input_file = input_file)

    for key, values in reader.read():
        for value in values:
            processed_value = map(None, value)

            #emit key-value pair
            print(f"{key}\t{processed_value}")