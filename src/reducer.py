# skeleton for mapper.py
# already have code to read input file from the $pwd/input/*.txt
# space to input hte mapper function
import os
import sys
sys.path.append("..")
from pathlib import Path
from reduce import reduce 
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

class Reducer(servicer.ReducerServicer):

    def __init__(self, port) -> None:
        super().__init__()
        self.input_files_paths = []
        self.port = port
        self.my_index = 0
        self.num_mapper = 0
        self.reducer = None
        self.my_path=os.path.dirname(os.path.realpath(__file__))

    def start(self):
        try:
            print(f"STARTING REDUCER AT PORT: {self.port}")
            self.reducer = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            servicer.add_ReducerServicer_to_server(self, self.reducer)
            print(f"REDUCER [{self.port}] STARTED")
            self.reducer.add_insecure_port(f"localhost:{self.port}")
            self.reducer.start()
            self.reducer.wait_for_termination()
        except KeyboardInterrupt:
            print(f"----------CLOSING REDUCER [{self.port}]---------")
            return
        except:
            print(f"----------CLOSING REDUCER [{self.port}]---------")
            return

    def StartReducer(self, request, context): # NotifyReducer
        self.num_mapper = request.num_mapper
        self.my_index = request.my_index
        self.input_files_paths = list(request.intermediate_paths)
        with open(f"{self.my_path}/input/input.txt", "a") as all_inter:
            for input_path in self.input_files_paths:
                with open(f'{input_path}/output{self.my_index}.txt') as inter:
                    for line in inter.readlines():
                        all_inter.write(line)
                    inter.close()
            all_inter.close()
        thread = Thread(target=self.do_reducing)
        thread.start()
        return empty_pb2.Empty()


    def do_reducing(self):
        """
        Here we will pass all the input file path to the map function that will take care of the rest
        send=> my_index and my_path/input 
        """
        reduce(self.my_index, f'{self.my_path}/input', self.num_mapper)
        # DO THE REQUIRED MAPPING WORK
        self.notify_master()


    def notify_master(self):
        master = grpc.insecure_channel('localhost:8880')
        notify_master_stub = servicer.MasterStub(master)
        response = notify_master_stub.NotifyMaster(
            messages.Response(response='REDUCER')
        )
        print(f"----------CLOSING REDUCER [{self.port}]---------")
        self.reducer.stop(None)


def main():
    reducer = Reducer(sys.argv[1])
    reducer.start()
    pass

if __name__=='__main__':
    try:
        main()
    except:
        exit

