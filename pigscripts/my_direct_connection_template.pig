/**
 * Customize this template script to begin
 * working with your Mongo data. This script
 * loads data from BSON files on S3 and saves back
 * to S3. 
 */

/******* Pig Script Parameters **********/

-- Mongo URI to retrieve input data from a Mongo collection
-- Uses secondary nodes preferentially
%default INPUT_COLLECTION 'your_input_collection';
%default INPUT_MONGO_URI 'mongodb://username:password@host:port/database.$INPUT_COLLECTION?readPreference=secondary';

-- Mongo URI to store output data to a Mongo collection
%default OUTPUT_COLLECTION 'your_output_collection';
%default OUTPUT_MONGO_URI 'mongodb://username:password@host:port/database.$OUTPUT_COLLECTION';

/******* mongo-hadoop settings **********/

-- To calculate input splits Hadoop makes a call that requires admin privileges in MongoDB 2.4+.
--
-- If you are connecting as a user with admin privileges you should remove this line for much better
-- performance.
SET mongo.input.split.create_input_splits false;

/******* Load Data **********/

/*
-- First time: load without schema
-- Puts all data into a single Pig Map field named document
mycollection = load '$INPUT_MONGO_URI' using com.mongodb.hadoop.pig.MongoLoader();
*/

/*
-- Once that works: load with schema
mycollection = load '$INPUT_MONGO_URI' using com.mongodb.hadoop.pig.MongoLoader(
                 'mongo_id:chararray',
                 'mongo_id');
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
-- Store data back to MongoDB
store counted into '$OUTPUT_MONGO_URI'
             using com.mongodb.hadoop.pig.MongoInsertStorage();
*/
