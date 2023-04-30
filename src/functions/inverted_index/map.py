import glob

"""
------------ INVERTED INDEX -------------
"""

def map(my_index, file_path, n_reducers):
    print(f'Mapper Index: {my_index}\tPath: {file_path}')
    input_files_paths = glob.glob(f"{file_path}/*.txt")

    for index in range(n_reducers):
        with open(f"{file_path}/../output/output{index+1}.txt", "a") as inter:
            inter.close()

    values = []
    for file in sorted(input_files_paths):
        document_id = file[-5]
        with open(file, 'r') as f:
            for line in f.readlines():
                words = list(line.strip().split(' '))
                values.extend(words)

        for word in values:
            index = partitioning_function(word, n_reducers)
            with open(f"{file_path}/../output/output{index+1}.txt", "a") as inter:
                inter.write(f"{word} {document_id}\n")
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


