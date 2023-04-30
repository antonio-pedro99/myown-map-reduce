# python3 /map/mapper.py
# getting map function and reduce function 
# modify the mapper with given map function => we will get a new mapper
# modify the reducer with given reduce function => we will get a new reduce
# send this mapper and reducer to different random nodes
# send the input files 

import os
# comming out of the master => to access the proto files
import sys
sys.path.append("..")
import grpc
from port import *
from concurrent import futures
from threading import Thread
from threading import Lock
import proto.map_reduce_pb2_grpc as servicer
from proto.map_reduce_pb2_grpc import MasterServicer, add_MasterServicer_to_server
import proto.map_reduce_pb2 as messages
from google.protobuf import empty_pb2
from pydantic import BaseModel
from pathlib import Path
import random
from time import sleep
import glob

class Config(BaseModel):
    input_path:str
    output_path:str
    n_reducers:int
    n_mappers:int


class Master(MasterServicer):
    def __init__(self,config:Config) -> None:
        super().__init__()
        self.config = config
        # appending the name if the nodes
        self.num_mapper_notified = 0
        self.num_reducer_notified = 0
        self.node_path_list=[]
        self.current_mapper_address=[]
        self.current_mapper_paths=[]
        self.current_reducer_paths=[]
        self.current_reducer_address=[]
        
        for i in range(1,6):
            self.node_path_list.append(f'nodes/node{i}')

    def transfer_mapper(self,num_mapper: int):
        nodes = random.sample(self.node_path_list,num_mapper)
        map_path = os.getcwd()+'/functions/map.py'
        mapper_path= os.getcwd()+'/mapper.py'
        for node in nodes:
            dest_path = f'{os.getcwd()}/../{node}/mapper/'
            self.current_mapper_paths.append(dest_path)
            os.system(f'cp {mapper_path} {dest_path}')
            os.system(f'cp {map_path} {dest_path}')

    def invoke_mappers(self):
        for mapper in self.current_mapper_paths:
            port = get_new_port()
            print(f'Invoking mapper at location: {mapper}')
            self.current_mapper_address.append(f'localhost:{port}')
            os.system(f'python3 {mapper}mapper.py {port} &')
            sleep(0.5)
    
    def divide_input_files(self, input_path):
        num_mapper = self.config.n_mappers
        path_to_send = [messages.NotifyMapper() for i in range(num_mapper)]
        input_files = glob.glob(f'{input_path}/*.txt')
        for i in range(len(input_files)):
            path_to_send[i%num_mapper].input_paths.append(input_files[i])
        return path_to_send

    def start_mapper(self):
        path_to_send = self.divide_input_files(self.config.input_path)
        for i,mapper_addr in enumerate(self.current_mapper_address):
            mapper = grpc.insecure_channel(mapper_addr)
            notify_mapper_stub = servicer.MapperStub(mapper)
            path_to_send[i].num_reducer=self.config.n_reducers
            path_to_send[i].my_index=(i+1)
            response = notify_mapper_stub.StartMapper(
                path_to_send[i]
            )


    def start_reducer(self):
        intermediate_path = [f'{path}output' for path in self.current_mapper_paths]
        for i,reducer_addr in enumerate(self.current_reducer_address):
            messages_to_send = messages.NotifyReducer()
            messages_to_send.my_index = (i+1)
            messages_to_send.num_mapper = self.config.n_mappers
            messages_to_send.intermediate_paths.extend(intermediate_path)
            reducer = grpc.insecure_channel(reducer_addr)
            notify_mapper_stub = servicer.ReducerStub(reducer)
            response = notify_mapper_stub.StartReducer(
                messages_to_send
            )

    def clear_mappers(self):
        print('[WARNING] clearing all the mappers')
        # clearing the mappers
        for mapper in self.current_mapper_paths:
            os.system(f'rm -rf {mapper}mapper.py')
            os.system(f'rm -rf {mapper}map.py')
            os.system(f'rm -rf {mapper}input/*.txt')
            os.system(f'rm -rf {mapper}output/*.txt')
        self.current_mapper_paths.clear()
        self.current_mapper_address.clear()


    def transfer_reducer(self,num_reducers: int):
        nodes = random.sample(self.node_path_list,num_reducers)
        reduce_path = os.getcwd()+'/functions/reduce.py'
        reducer_path= os.getcwd()+'/reducer.py'
        for node in nodes:
            dest_path = f'{os.getcwd()}/../{node}/reducer/'
            self.current_reducer_paths.append(dest_path)
            os.system(f'cp {reducer_path} {dest_path}')
            os.system(f'cp {reduce_path} {dest_path}')

    def invoke_reducer(self):
        for reducer in self.current_reducer_paths:
            port = get_new_port()
            print(f'Invoking reducer at location: {reducer}')
            self.current_reducer_address.append(f'localhost:{port}')
            os.system(f'python3 {reducer}reducer.py {port} &')
            sleep(0.5)

    def clear_reducer(self):
        print('[WARNING] clearing all the reducers')
        # clearing the mappers
        for reducer in self.current_reducer_paths:
            os.system(f'rm -rf {reducer}reducer.py')
            os.system(f'rm -rf {reducer}reduce.py')
            os.system(f'rm -rf {reducer}input/*.txt')
            os.system(f'rm -rf {reducer}output/*.txt')
            os.system(f'rm -rf {self.config.output_path}/*.txt')
        self.current_reducer_paths.clear()
        self.current_reducer_address.clear()
    
    def collect_outputs(self):
        for path in self.current_reducer_paths:
            os.system(f'cp {path}output/output*.txt {self.config.output_path}/')

    def handle_mappers(self):
        # tranfering required number of mappers to the nodes
        self.transfer_mapper(self.config.n_mappers)
        self.invoke_mappers()
        self.start_mapper()

    def handle_reducer(self):
        # tranfering required number of reducers to the nodes
        self.transfer_reducer(self.config.n_reducers)
        self.invoke_reducer()
        self.start_reducer()
        input("Press enter to clear both mapper and reducer")
        self.clear_mappers()
        self.clear_reducer()

    def start(self):
        try:
            print("STARTING MASTER WITH THE FOLLOWING CONFIGURATIONS: ")
            print(f"Input Location = {self.config.input_path}"+
                  f"\nOutput Location = {self.config.output_path}"+
                  f"\nMappers = {self.config.n_mappers}\nReducers = {self.config.n_reducers}")
        
            thread = Thread(target=self.handle_mappers)
            thread.start()

            master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            add_MasterServicer_to_server(self, master_server)
            print("MASTER STARTED")
            master_server.add_insecure_port("localhost:8880")
            master_server.start()
            master_server.wait_for_termination()
        except KeyboardInterrupt:
            master_server.stop(None)
            print("----------CLOSING MASTER---------")
            return

    def NotifyMaster(self, request, context):
        print('NOTIFIED MASTER OF COMPLETION')
        if request.response == 0:
            self.num_mapper_notified += 1
        if request.response == 1:
            self.num_reducer_notified += 1
        if self.num_mapper_notified == self.config.n_mappers:
            self.num_mapper_notified=0
            thread = Thread(target = self.handle_reducer)
            thread.start()
        if self.num_reducer_notified == self.config.n_reducers:
            self.num_reducer_notified=0
            self.collect_outputs()
            print('ALL DONE')
        return empty_pb2.Empty()
    
    
def main():
    input_path = input("Enter the input data location: ")
    # input_path = '/home/dscd/map-reduce/src/user_input/nj'
    output_path = input("Enter the output data location: ")
    if output_path=='':
        output_path = '/home/dscd/map-reduce/src/user_output'
    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    
    input_data = Config(
        input_path=input_path,
        output_path=output_path,
        n_mappers=num_mappers,
        n_reducers=num_reducers
    )
    
    master = Master(config = input_data)
    master.start()
    

if __name__=="__main__":
    main()