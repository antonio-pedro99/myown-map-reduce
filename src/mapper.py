# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("..")
from pathlib import Path
from map import map, partitioning_function
from google.protobuf import empty_pb2
import glob
import grpc
from time import sleep
from threading import Thread
from concurrent import futures
import proto.map_reduce_pb2_grpc as servicer
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2 as EmptyResponse
from utils.record_reader import RecordReader


def partition(key, n_reducers):
    return hash(key) % n_reducers

class Mapper(servicer.MapperServicer):

    def __init__(self, port) -> None:
        super().__init__()
        self.input_files_paths = None
        self.port = port
        self.num_reducer = 0
        self.mapper = None

    def start(self):
        try:
            print(f"STARTING MAPPER AT PORT: {self.port}")
            self.mapper = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            servicer.add_MapperServicer_to_server(self, self.mapper)
            print(f"MAPPER [{self.port}] STARTED")
            self.mapper.add_insecure_port(f"localhost:{self.port}")
            self.mapper.start()
            self.mapper.wait_for_termination()
        except KeyboardInterrupt:
            print(f"----------CLOSING MAPPER [{self.port}]---------")
            return
        except:
            print(f"----------CLOSING MAPPER [{self.port}]---------")
            return

    def StartMapper(self, request, context):
        self.num_reducer = request.num_reducer
        self.input_files_paths = list(request.input_paths)
        print(f'path received by {self.port}: \n{request.input_paths}\n')
        thread = Thread(target=self.do_mapping)
        thread.start()
        return empty_pb2.Empty()


    def do_mapping(self):
        # DO THE REQUIRED MAPPING WORK
        self.notify_master()


    def notify_master(self):
        sleep(1)
        master = grpc.insecure_channel('localhost:8880')
        notify_master_stub = servicer.MasterStub(master)
        response = notify_master_stub.NotifyMaster(
            messages.Response(response='SUCCESS')
        )
        self.mapper.stop(None)
        print(f"----------CLOSING MAPPER [{self.port}]---------")


    def get_input(self):
        # get input from input files
        pass


def main():
    mapper = Mapper(sys.argv[1])
    mapper.start()

if __name__=='__main__':
    try:
        main()
    except:
        exit
    # input_files_paths = glob.glob("input/*.txt")
    # #input_file = os.path.join('input', 'Input1.txt')
    # #reader = RecordReader(input_file = input_file)

    # """
    # python3 mapper.py 53240
    # Mapper function
  
    # """

    # id = 1
    # for file in sorted(input_files_paths):
    #     reader = RecordReader(input_file = file)
    #     key_values = []

       
    #     for key, values in reader.read():
           
    #         for v in values:
    #             key_values.extend(map(id, v))

    #     print(key_values)
    #     for k_v in key_values:
    #             index = partition(k_v[0], 2)
            
    #             with open(f"output/output{index}.txt", "a") as inter:
    #                 k, v = k_v
    #                 inter.write(f"{k}, {v}\t\n")
    #                 inter.close()
    #     id += 1

