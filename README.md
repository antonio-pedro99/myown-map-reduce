# map-reduce
A Simple implementation of map reduce. 


# Generate 
python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=./src ./proto/map_reduce.proto