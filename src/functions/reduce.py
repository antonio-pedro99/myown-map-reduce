"""
----------------- WORD-COUNT -----------------
"""

def reduce(my_index, input, num_mapper):
    print(f'Reducer Index: {my_index} Path: {input}')
    file = f'{input}/input.txt'
    all_kv_pairs = []
    with open(file, 'r') as f:
        for line in f.readlines():
            pairs = tuple(line.strip().split(' '))
            all_kv_pairs.append(pairs)
    
    # key sorting [lower and uppercase words are considered same] 
    all_kv_pairs = sorted(all_kv_pairs, key=lambda x: x[0].lower() )

    key_values={}
    current = ''
    # compiling 
    for kv in all_kv_pairs:
        if current.lower() != kv[0].lower():
            current = kv[0]
            key_values[current]=[]    
        key_values[current].append(int(kv[1]))
    
    # reducing 
    for key in key_values.keys():
        key_values[key] = sum(key_values[key])

    # writing to the output file
    out_file = f'{input}/../output/output{my_index}.txt'
    with open(out_file, 'a') as out:
        for key,val in key_values.items():
            out.write(f'{key} {val}\n')
    out.close()