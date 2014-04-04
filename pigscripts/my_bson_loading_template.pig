/**
 * Customize this template script to begin
 * working with your Mongo data. This script
 * loads data from BSON files on S3 and saves back
 * to S3. 
 */

/******* Pig Script Parameters **********/

-- Input path to either a single BSON file
-- or a directory with multiple BSON files from the same collection
-- (Do not point to files from different collections)
%default INPUT_PATH 's3://my-input-bucket/my-folder/mycollection.bson';

-- Path to store output
%default OUTPUT_PATH 's3://my-output-bucket/my-folder/output';


/******* mongo-hadoop settings **********/

-- Prevent mongo-hadoop from writing metadata about BSON files
-- to your input S3 bucket. Generally, this is a good practice
-- in case you don't have write permissions to the bucket
-- where your input BSON is stored.
set bson.split.write_splits false;

-- Filter to only find files with extension .bson when scanning
-- directories for which files to process
set bson.pathfilter.class com.mongodb.hadoop.BSONPathFilter;

/******* Load Data **********/

/*
-- First time: load without schema
-- Puts all data into a single Pig Map field named document
mycollection = load '$INPUT_PATH' using com.mongodb.hadoop.pig.BSONLoader();
*/

/*
-- Once that works: load with schema
mycollection = load '$INPUT_PATH' using com.mongodb.hadoop.pig.BSONLoader(
                 'mongo_id',
                 'mongo_id:chararray');
*/

/***** Process Data *********/

/*
-- Example processing step: count number of rows in collection
-- Replace with your processing
grouped = group mycollection all;
counted = foreach grouped generate
			COUNT(mycollection);
*/

/******* Store Data **********/

/*
-- Remove any existing output and store data to S3
rmf $OUTPUT_PATH;
store counted into '$OUTPUT_PATH' using PigStorage();
*/