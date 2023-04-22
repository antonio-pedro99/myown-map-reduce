def map(key, value):
    """
        Define your mapper logic here. 
        The following code applies only on inverted index** for testing purpose
        e.g:

        key - document id
        value - document text

    """

    terms = value.split()
    pairs = set()
    for term in terms:
        pairs.add((term, key))
    return pairs
