# map-reduce
A Simple implementation of map reduce. 

## How to run the map-reduce program:
- Copy the relevant map.py and reduce.py in the functions folder
- run "python3 master.py" to run the program

### Generate 
python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=./src ./proto/map_reduce.proto

### For Compiling Proto file
- for inner
python3 -m grpc_tools.protoc -I./ --python_out=./src --pyi_out=./src --grpc_python_out=./src ./proto/map_reduce.proto
- for outer
python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. ./proto/map_reduce.proto