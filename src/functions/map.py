import glob

"""
------------ WORD COUNT -------------
"""

def map(my_index, file_path, n_reducers):
    print(f'Mapper Index: {my_index}\tPath: {file_path}')
    input_files_paths = glob.glob(f"{file_path}/*.txt")
    values = []
    for file in sorted(input_files_paths):
        with open(file, 'r') as f:
            for line in f.readlines():
                words = list(line.strip().split(' '))
                values.extend(words)

    for word in values:
        index = partitioning_function(word, n_reducers)
        with open(f"{file_path}/../output/output{index+1}.txt", "a") as inter:
            inter.write(f"{word} 1\n")
            inter.close()
    pass


# def map(key, value):
#     """
#         Define your mapper logic here. 
#         The following code applies only on inverted index** for testing purpose
#         e.g:

#         key - document id
#         value - document text
#         return a list of key:value tuples like [(key1, value1),(key2, value2),...]
#         if there is only one tuple than also return as a list like [(key1, value1)]
#     """
#     # this is example of word count
#     """  terms = value.split(' ')
#     pairs=[]
#     for term in terms:
#         pairs.append((term, 1))
#     return pairs """
#     words = value.split()
#     pairs = [(word, key) for word in words]
#     return pairs
    

def partitioning_function(key, n_reducers):
    """
        This method is used to partition the outputs for reducer.
        This will be applied on the key value above
        eg. 
        For Word Count => using length of word as partitioning function
    """
    return len(key) % n_reducers


# class RecordReader:
#     def __init__(self, input_file):
#         self.input_file = input_file
#         self.current_key = None
#         self.current_value = None

#     def read(self):
#         with open(self.input_file, 'r') as f:
#             for line in f.readlines():
#                 parts = line.strip().split('\t')
#                 print(parts)
#                 key = parts[0]
#                 value = parts[-1]

#                 if self.current_key is not None and key != self.current_key:
#                     yield self.current_key, self.current_value
#                     self.current_value = None

#                 self.current_key = key
#                 if self.current_value is None:
#                     self.current_value = []
#                 self.current_value.append(value)
#         if self.current_key is not None:
#             yield self.current_key, self.current_value

