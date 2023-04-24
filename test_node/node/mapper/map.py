def map(input_value):
    """
        Define your mapper logic here. 
        The following code applies only on inverted index** for testing purpose
        e.g:

        key - document id
        value - document text
        return a list of key:value tuples like [(key1, value1),(key2, value2),...]
        if there is only one tuple than also return as a list like [(key1, value1)]
    """
    # this is example of word count
    terms = input_value.split(' ')
    pairs=[]
    for term in terms:
        pairs.append((term, 1))
    return pairs
    

def partitioning_function(value):
    """
        This method is used to partition the outputs for reducer.
        This will be applied on the key value above
        eg. 
        For Word Count => using length of word as partitioning function
    """
    return len(value) 