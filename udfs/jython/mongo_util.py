"""
    Copyright 2013 Mortar Data Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""


@outputSchema('mongo_data:bag{t:(keyname:chararray, type:chararray, val:chararray)}')
def mongo_map(d, prefix=""):
    """
    Go through a dictionary and for every key record the key name, type, and data value.

    Recursively goes through embedded lists/dictionaries and prepends parent keys to the key name.
    """
    output = []
    for k,v in d.iteritems():
        key_name = "%s%s" % (prefix, k)

        if type(v) == list:
            output.append( (key_name, type(v).__name__, type(v).__name__) )
            for t in v:
                for t_item in t:
                    if type(t_item) == dict:
                        output += mongo_map(t_item, "%s." % key_name)
        elif type(v) == dict:
            output.append( (key_name, type(v).__name__, type(v).__name__) )
            output += mongo_map(v, "%s." % key_name)
        else:
            #For simple types, keep example values
            output.append( (key_name, type(v).__name__, "%s" % v) )
    return output

@outputSchema('schema:chararray')
def create_mongo_schema(results, prefix_to_remove=""):
    """
    Create a schema string that can be used by the MongoLoader for this collection.

    results: List of keyname with ordered counts of the type:
         [ (keyname, [ (type1, count1), (type2, count2) ]),
           (keyname, [ (type1, count1), (type2, count2) ]), ...  ]
    prefix_to_remove: String to remove from keyname.
    """
    params = []
    index = 0
    while index < len(results):
        t = results[index]
        full_key_name = t[0]
        short_key_name = t[0].replace(prefix_to_remove, "")
        key_type_counts = t[1]
        key_type_counts.sort(key=lambda x: x[1])
        key_type = key_type_counts[0][1]
        if key_type == 'NoneType':
            if len(key_type_counts) > 1:
                key_type = key_type_counts[1][1]
            else:
                #Default to loading field as a string
                key_type = "unicode"

        if key_type == 'list':
            inner_params = []
            index += 1
            while index < len(results) and results[index][0].startswith(full_key_name):
                inner_params.append(results[index])
                index += 1
            inner_schema = create_mongo_schema(inner_params, "%s%s." % (prefix_to_remove, short_key_name))
            param = "%s:bag{t:tuple(%s)}" % (short_key_name, inner_schema)
        elif key_type == 'dict':
            inner_params = []
            index += 1
            while index < len(results) and results[index][0].startswith(full_key_name):
                inner_params.append(results[index])
                index += 1
            inner_schema = create_mongo_schema(inner_params, "%s%s." % (prefix_to_remove, short_key_name))
            param = "%s:tuple(%s)" % (short_key_name, inner_schema)
        else:
            pig_key_type = _get_pig_type(key_type)
            param =  "%s:%s" % (short_key_name, pig_key_type)
            index += 1

        params.append(param)

    depth = "\t" * prefix_to_remove.count(".")
    join_str = ",\n%s" % depth
    schema = "\n%s%s" % (depth, join_str.join(params))

    #Print out final schema but not intermediate ones.
    #This allows a schema to be printed from a single document with illustrate
    if not prefix_to_remove:
        print schema

    return schema

def _get_pig_type(python_type):
    if python_type == 'unicode':
        return 'chararray'
    elif python_type == 'bytearray':
        return 'bytearray'
    elif python_type == 'long':
        return 'long'
    elif python_type == 'int':
        return 'int'
    elif python_type == 'float':
        return 'double'
    else:
        return 'unknown'