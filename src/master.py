# python3 /map/mapper.py
# getting map function and reduce function 
# modify the mapper with given map function => we will get a new mapper
# modify the reducer with given reduce function => we will get a new reduce
# send this mapper and reducer to different random nodes
# send the input files 

import os
li = []
with open("map.py", "r") as _map:
    for line in _map.readlines():
        li.append(line)

new_mapper = open("new_mapper.py", "w")
for line in li:
    new_mapper.write
with open("mapper.py", "r") as mapper:
    new_mapper.writelines(li)
