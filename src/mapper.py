# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("..")
from pathlib import Path
from map import map
from google.protobuf import empty_pb2
import glob
import grpc
from time import sleep
from threading import Thread
from concurrent import futures
import proto.map_reduce_pb2_grpc as servicer
import proto.map_reduce_pb2 as messages


def partition(key, n_reducers):
    return hash(key) % n_reducers

class Mapper(servicer.MapperServicer):

    def __init__(self, port) -> None:
        super().__init__()
        self.input_files_paths = []
        self.port = port
        self.my_index = 0
        self.num_reducer = 0
        self.mapper = None
        self.my_path=os.path.dirname(os.path.realpath(__file__))

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
        self.my_index = request.my_index
        self.input_files_paths = list(request.input_paths)
        # print(f'path received by {self.port}: \n{request.input_paths}\n')
        for input_path in self.input_files_paths:
            os.system(f'cp {input_path} {self.my_path}/input/')
        thread = Thread(target=self.do_mapping)
        thread.start()
        return empty_pb2.Empty()


    def do_mapping(self):
        """
        Here we will pass all the input file path to the map function that will take care of the rest
        send=> my_index and my_path/input 
        """
        map(self.my_index, f'{self.my_path}/input', self.num_reducer)
        # DO THE REQUIRED MAPPING WORK
        self.notify_master()


    def notify_master(self):
        master = grpc.insecure_channel('localhost:8880')
        notify_master_stub = servicer.MasterStub(master)
        response = notify_master_stub.NotifyMaster(
            messages.Response(response='MAPPER')
        )
        print(f"----------CLOSING MAPPER [{self.port}]---------")
        self.mapper.stop(None)


def main():
    mapper = Mapper(sys.argv[1])
    mapper.start()

if __name__=='__main__':
    try:
        main()
    except:
        exit

