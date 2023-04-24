# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("../../..")
from pathlib import Path
import glob
from reduce import reduce
import proto.map_reduce_pb2_grpc as servicer
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader
from collections import defaultdict

def get_input():
    """
        Get all inputs from the input folder

    """
    pass


def shuffle(n_partitions):
    """
        To shuffle the output of intermediate results from the mapper
        generic function, to handle all the situation.
    """
    intermediate_results = []
    for r in range(n_partitions):
        intermediate_result_file = f"input/input{r}.txt"

        with open(intermediate_result_file, 'r') as file:
                file_contents = file.readlines()
                values = map(lambda line: tuple(line.strip().split(',')), file_contents)
                intermediate_results.extend(values)
        
        groups = defaultdict(list)

        for key, value in intermediate_results:
            groups[key].append(value)
        
        out_file = f"output/intermediate{r}.txt"
        with open(out_file, 'w') as f:
            for key, values in groups.items():
                for value in values:
                    f.write(f"{key} {value}\n")


#RPC call
def StartReducer():
    pass


if __name__=='__main__':
    intermediate_result_paths = glob.glob("input/*.txt")
    
    R = 2 #to be changed
    #shuffle the results first
    shuffle(R)

    #reduce phase
    final_output = []
    grouped_data = {}
    for r in range(R):
        intermdiate_result_file = f"output/intermediate{r}.txt"
       
        with open(intermdiate_result_file, "r") as f:
            grouped_values = [line.strip().split() for line in f]
    
        for key, value in grouped_values:
            if key in grouped_data:
                grouped_data[key].append(value)
            else:
                grouped_data[key] = [value]
                
        for k, values in grouped_data.items():
            reducer_result = reduce(k, values)
            final_output.append(reducer_result)
    

    for r in range(R):
        output_file = f"output/output{r}.txt"
        for _key, _value in final_output:
            with open(output_file, "a") as f:
                f.write(f"{_key} {_value}\t\n")
