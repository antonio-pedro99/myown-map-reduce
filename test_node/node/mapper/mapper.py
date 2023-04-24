# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("../../..")
from pathlib import Path
from map import map, partitioning_function
import glob
import proto.map_reduce_pb2_grpc as servicer
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader


def partition(key, n_reducers):
    value = hash(key)
    return value % n_reducers

class Mapper(servicer.MapperServicer):

    def __init__(self) -> None:
        super().__init__()
        self.input_files_paths = glob.glob("input/*.txt")

    # RPC call
    def StartMapper(self, request, context):
        pass

    def get_input(self):
        # get input from input files
        pass



if __name__=='__main__':
    input_files_paths = glob.glob("input/*.txt")
    #input_file = os.path.join('input', 'Input1.txt')
    #reader = RecordReader(input_file = input_file)

    """
    python3 mapper.py 53240
    Mapper function
  
    """

    for file in input_files_paths:
        reader = RecordReader(input_file = file)
        key_values = []

        for key, values in reader.read():
           
            for v in values:
                key_values.extend(map(v))
      
        for k_v in key_values:
                index = partition(k_v[0], 2)
            
                with open(f"output/output{index}.txt", "a") as inter:
                    k, v = k_v
                    inter.write(f"{k}, {v}\t\n")
                    inter.close()

