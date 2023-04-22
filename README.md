# map-reduce
A Simple implementation of map reduce. 


# Generate 
python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=./src ./proto/map_reduce.proto

# For Rahul
- for inner
python3 -m grpc_tools.protoc -I./ --python_out=./src --pyi_out=./src --grpc_python_out=./src ./proto/map_reduce.proto
- for outer
python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. ./proto/map_reduce.proto