import numpy as np
"""
----------------- NATURAL JOIN -----------------
"""

def reduce(my_index, input, num_mapper):
    print(f'Reducer Index: {my_index} Path: {input}')
    file = f'{input}/input.txt'
    all_kv_pairs = []
    with open(file, 'r') as f:
        for line in f.readlines():
            row = tuple(line.strip().split(' '))
            all_kv_pairs.append(row)
    
    # key sorting [lower and uppercase words are considered different] 
    all_kv_pairs = sorted(all_kv_pairs, key=lambda x: x[0])


    key_values={}
    current = ''
    # compiling 
    for kv in all_kv_pairs:
        if current != kv[0]:
            current = kv[0]
            key_values[current]=[]    
        key_values[current].append(tuple([kv[1],kv[2]]))
    
    final_tuples = []
    final_tuples.append(list(['Name', 'Age', 'Role']))
    # reducing
    for key in key_values.keys():
        current_list = key_values[key]
        first_list = []
        second_list = []
        for row in current_list:
            if row[0]=='1':
                first_list.append(row[1])
            else:
                second_list.append(row[1])
        for first in first_list:
            for second in second_list:
                final_tuples.append(list([key, first, second]))

    # writing to the output file
    out_file = f'{input}/../output/output{my_index}.txt'
    with open(out_file, 'a') as out:
        for row in final_tuples:
            out.write(f'{", ".join(row)}\n')
    out.close()