# python3 /map/mapper.py
# getting map function and reduce function 
# modify the mapper with given map function => we will get a new mapper
# modify the reducer with given reduce function => we will get a new reduce
# send this mapper and reducer to different random nodes
# send the input files 

import os
import grpc

from concurrent import futures
from threading import Lock
from proto.map_reduce_pb2_grpc import MasterServicer, add_MasterServicer_to_server, MasterStub
from proto.map_reduce_pb2 import Response, Notification
from google.protobuf import empty_pb2 as EmptyResponse
from pydantic import BaseModel
from pathlib import Path

class Config(BaseModel):
    input_path:str
    output_path:str
    n_reducers:int
    n_mappers:int


class Master(MasterServicer):
    def __init__(self,config:Config) -> None:
        super().__init__()
        self.config = config

    def start(self):
        try:
            print("STARTING MASTER WITH THE FOLLOWING CONFIGURATIONS: ")
            print(f"Input Location = {self.config.input_path}"+
                  f"\nOutput Location = {self.config.output_path}"+
                  f"\nMappers = {self.config.n_mappers}\nReducers = {self.config.n_reducers}")
        
            master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            add_MasterServicer_to_server(self, master_server)
            print("MASTER STARTED")
            master_server.add_insecure_port("localhost:8880")
            master_server.start()
            master_server.wait_for_termination()
        except KeyboardInterrupt:
            print("----------CLOSING MASTER---------")
            return
        
    def StartMapper(self, request:Notification, context)->EmptyResponse:
        return super().StartMapper(request, context)
    
    def StartReducer(self, request:Notification, context)->EmptyResponse:
        return super().StartReducer(request, context)

    def NotifyMaster(self, request:EmptyResponse, context)->EmptyResponse:
        return super().NotifyMaster(request, context)
    
def main():
    input_path = input("Enter the input data location: ")
    output_path = input("Enter the output data location: ")
    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    
    input_data = Config(
        input_path=input_path,
        output_path=output_path,
        n_mappers=num_mappers,
        n_reducers=num_reducers
    )
    
    master = Master(config = input_data)
    os.system("python3 mapper.py")
    master.start()
    

if __name__=="__main__":
    main()