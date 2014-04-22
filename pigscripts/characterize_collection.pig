/*
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
*/

/*
    This pig script will return some basic information about a MongoDB collection.  Output is:

    1. Field Name.  Embedded fields have their parent's field name prepended to their name.
            Every field that appears in any document in the collection is listed.
    2. Unique value count.  The number of unique values associated with the field.
    3. Example value.  An example value for the field.
    4. Example value type.  The data type of the example value.
    5. Value count.  The number of times the example value appeared for this field in the collection

    Each field is listed up to five times with their five most common example values.
 */

REGISTER '../udfs/jython/mongo_util.py' USING jython AS mongo_util;

/*
 To calculate input splits Hadoop makes a call that requires admin privileges in MongoDB 2.4+.

 If you are connecting as a user with admin privileges you should remove this line for much better
 performance.
*/
SET mongo.input.split.create_input_splits false


data = LOAD '$MONGO_URI.$INPUT_COLLECTION'
       USING com.mongodb.hadoop.pig.MongoLoader();

-- Create one row for every field in the document
raw_fields =  FOREACH data
             GENERATE flatten(mongo_util.mongo_map(document));

-- Group the rows by field name and find the number of unique values for each field in the collection
key_groups = GROUP raw_fields BY (keyname);
unique_vals = FOREACH key_groups {
    v = raw_fields.val;
    unique_v = distinct v;
    GENERATE flatten(group)  as keyname:chararray,
             COUNT(unique_v) as num_vals_count:long;
}

-- Find the number of times each value occurs for each field
key_val_groups = GROUP raw_fields BY (keyname, type, val);
key_val_groups_with_counts =  FOREACH key_val_groups
                             GENERATE flatten(group),
                                      COUNT($1) as val_count:long;

-- Find the top 5 most common values for each field
key_vals = GROUP key_val_groups_with_counts BY (keyname);
top_5_vals = FOREACH key_vals {
    ordered_vals = ORDER key_val_groups_with_counts BY val_count DESC;
    limited_vals = LIMIT ordered_vals 5;
    GENERATE flatten(limited_vals);
}

-- Join unique vals with top 5 values
join_result = JOIN unique_vals BY keyname,
                   top_5_vals  BY keyname;

-- Clean up columns (remove duplicate keyname field)
result =  FOREACH join_result
         GENERATE unique_vals::keyname as keyname,
                  num_vals_count,
                  val,
                  type,
                  val_count;

-- Sort by field name and number of values
out = ORDER result BY keyname, val_count DESC;

-- Store data to output collection in MongoDB
STORE out INTO '$MONGO_URI.$OUTPUT_COLLECTION'
             USING com.mongodb.hadoop.pig.MongoInsertStorage('');


-- Uncomment to store to S3 instead of MongoDB
--rmf s3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out;
--STORE out INTO 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/meta_out'
--         USING PigStorage('\t');