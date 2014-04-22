/*
 *  This is an example pig script, showing different ways to connect to
 *  and process MongoDB data. It will go through a small sampling of a 
 *  single day's worth of Tweets and count the number of times a word 
 *  was tweeted bucketed into two hour time blocks of the
 *  tweeter's local time.
 */

register '../udfs/jython/timeutils.py' using jython as timeutils;

/******* Pig Script Parameters **********/

%default INPUT_PATH '../data/tweets.bson';
%default OUTPUT_PATH '../data/tweets_by_time_block';

/******* mongo-hadoop settings **********/

-- Prevent mongo-hadoop from writing metadata about BSON files
-- to your input S3 bucket. Generally, this is a good practice
-- in case you don't have write permissions to the bucket
-- where your input BSON is stored.
set bson.split.write_splits false;

-- Filter to only find files with extension .bson when scanning
-- directories for which files to process
set bson.pathfilter.class com.mongodb.hadoop.BSONPathFilter;

-- To calculate input splits Hadoop makes a call that requires admin privileges in MongoDB 2.4+.
-- If you are connecting as a user with admin privileges you should remove this line for much better
-- performance.
set mongo.input.split.create_input_splits false;

/******* Load Data **********/

-- Load tweets from MongoDB
tweets =  LOAD 'mongodb://readonly:readonly@ds035147.mongolab.com:35147/twitter.tweets'
         USING com.mongodb.hadoop.pig.MongoLoader('created_at:chararray, text:chararray, user:tuple(utc_offset:int)');


-- Uncomment to load tweets from a mongodump backup stored in S3
-- tweets =  load 's3://mortar-example-data/twitter-mongo/tweets.bson' using com.mongodb.hadoop.pig.BSONLoader(
--             'tweet_mongo_id',
--             'created_at:chararray,
--              text:chararray,
--              user:tuple(utc_offset:int)');

/******* Perform Calculations **********/

-- Find the tweets that mention our search term and have valid time information
filtered_tweets = filter tweets
                      by text matches '.*[Ee]xcite.*'
                         and user.utc_offset is not null;

-- Calculate local time for each tweet
tweets_with_local_time = foreach filtered_tweets generate 
        timeutils.local_time(created_at, user.utc_offset) as created_at_local_tz_iso;

-- Calculate hour bucket for each tweet
tweets_with_time_buckets = foreach tweets_with_local_time generate
        timeutils.hour_block(created_at_local_tz_iso) as hour_block;

-- Group and count the number of tweets by hour bucket
grouped = group tweets_with_time_buckets by hour_block;
counted =  foreach grouped generate 
              group as hour_block,
              COUNT(tweets_with_time_buckets.hour_block) as num_tweets;

-- Sort them in ascending order
ordered = order counted BY hour_block asc;

/******* Store Results **********/

rmf $OUTPUT_PATH/$MORTAR_EMAIL_S3_ESCAPED;
store ordered into '$OUTPUT_PATH/$MORTAR_EMAIL_S3_ESCAPED' using PigStorage();
