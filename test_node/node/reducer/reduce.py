def reduce(key, values):
    """
    
        Define your reduce function here.

    """

    """  sum_values = 0
    for value in values:
        sum_values += int(value)
    
    return key, sum_values """
    print(values)
    doc_ids = list(set(pair[-1] for pair in values))
    return (key, (doc_ids))
