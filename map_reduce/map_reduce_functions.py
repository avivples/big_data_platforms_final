import warnings

import pandas as pd

warnings.filterwarnings('ignore')


def inverted_map(document_name):
    """
    Reads the csv document from the local disk and return a list that contains
    entries of the form (key_value, document name)

    Parameters:
    document_name (csv): csv document name to run the map function on
    column_index (int): index of column to create list for

    Returns:
    entries (lst): list of tuples of the defined form
    """
    entries = []
    df = pd.read_csv(document_name)
    for _, row in df.iterrows():
        for col in df.columns:
            entries.append(("_".join([col, row[col]]), document_name))
    return entries


def inverted_reduce(value, documents):
    """
    Take the two parameters given and put them together in a list and get rid of
    the duplicates in the parameter documents

    Parameters:
    value (str): column name and value in the form of columnname_value
    documents(str): list of all csv documents per given value

    Returns:
    lst: concatenation of value and the list of csv documents with no duplicates
    """
    return [value] + list(set(documents.split(",")))
