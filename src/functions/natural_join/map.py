import glob

"""
------------ NATURAL JOIN -------------
"""

def map(my_index, file_path, n_reducers):
    print(f'Mapper Index: {my_index}\tPath: {file_path}')
    input_files_paths = glob.glob(f"{file_path}/*.txt")
    values = []

    for index in range(n_reducers):
        with open(f"{file_path}/../output/output{index+1}.txt", "a") as inter:
            inter.close()
    
    for file in sorted(input_files_paths):
        ignore_firstline = True
        document_id = file[-5]
        with open(file, 'r') as f:
            for line in f.readlines():
                if ignore_firstline:
                    ignore_firstline = False
                    continue
                words = tuple(line.strip().split(', '))
                values.append(words)

        for word in values:
            index = partitioning_function(word[0], n_reducers)
            with open(f"{file_path}/../output/output{index+1}.txt", "a") as inter:
                inter.write(f"{word[0]} {document_id} {word[1]}\n")
                inter.close()

        values.clear()
    pass
    

def partitioning_function(key, n_reducers):
    """
        This method is used to partition the outputs for reducer.
        This will be applied on the key value above
        eg. 
        For Word Count => using length of word as partitioning function
    """
    return len(key) % n_reducers