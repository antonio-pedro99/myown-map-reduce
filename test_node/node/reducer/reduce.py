def reduce(key, values):
    """
    
        Define your reduce function here.

    """

    sum_values = 0
    for value in values:
        sum_values += int(value)
    
    return key, sum_values
