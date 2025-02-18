import mgp
import json

@mgp.function
def merge(map_a: mgp.Map, map_b: mgp.Map) -> mgp.Map:
    map_a.update(map_b)
    return map_a


@mgp.function
def to_set(collection: list) -> list:
    seen = set()  
    result = []   
    
    for item in collection:

        item_frozenset = frozenset(item.items())
        

        if item_frozenset not in seen:
            seen.add(item_frozenset)
            result.append(item)
    
    return result


@mgp.function
def sort(collection: list) -> list:
    sorted_list = list(collection)
    sorted_list.sort()
    return sorted_list


@mgp.function
def flatten(collection: list) -> list:
    new_list = []
    for sub_collection in collection:
        sub_collection = list(sub_collection)
        for element in sub_collection:
            new_list.append(element)
    return new_list
    
@mgp.function
def join_collection(collection: list, separator: str = '') -> str:
    result = ''
    for i, item in enumerate(collection):
        if i == 0:
            result += str(item)  
        else:
            result += separator + str(item)

    return result
@mgp.function
def json_to_cypher(json_list):
    cypher_list = "[" + ", ".join(map(str, json_list)) + "]"
    return cypher_list
@mgp.function
def text_replace(string, old, new):
    return string.replace(old, new)
@mgp.function
def text_join(elements, delimiter):
    return delimiter.join(elements)
@mgp.function
def text_split(string, delimiter):
    return string.split(delimiter)

