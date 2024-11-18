def remove_none_values(dict):
    return {k:v for k,v in dict.items() if v is not None}