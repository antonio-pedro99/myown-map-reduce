# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("../../..")
from pathlib import Path
from map import *
import glob
import proto.map_reduce_pb2_grpc as servicer
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader

class mapper(servicer.MapperServicer):

    def __init__(self) -> None:
        super().__init__()
        self.input_files_paths = glob.glob("input/*.txt")

    # RPC call
    def StartMapper(self, request, context):
        pass

    def get_input():
        # get input from input files
        pass



if __name__=='__main__':
    print(glob.glob("input/*.txt") )
    input_file = os.path.join('input', 'Input1.txt')
    reader = RecordReader(input_file = input_file)

    for key, values in reader.read():
        for value in values:
            processed_value = map(None, value)

            #emit key-value pair
            print(f"{key}\t{processed_value}")