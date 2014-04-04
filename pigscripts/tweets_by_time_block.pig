/*
 *  This is an example pig script, showing different ways to connect to
 *  and process MongoDB data. It will go through a small sampling of a 
 *  single day's worth of Tweets and count the number of times a word 
 *  was tweeted bucketed into two hour time blocks of the
 *  tweeter's local time.
 */

register '../udfs/python/timeutils.py' using streaming_python as timeutils;

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

/******* Load Data **********/

-- Load up the tweets from a mongodump backup stored in S3
tweets =  load '$INPUT_PATH' using com.mongodb.hadoop.pig.BSONLoader(
             'tweet_mongo_id',
             'created_at:chararray,
              text:chararray,
              user:tuple(utc_offset:int)');

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

rmf $OUTPUT_PATH;
store ordered into '$OUTPUT_PATH/$MORTAR_EMAIL_S3_ESCAPED' using PigStorage();
