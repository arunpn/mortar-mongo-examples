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

from pig_util import outputSchema

import time
import datetime

@outputSchema('local_time:chararray')
def local_time(twitter_ts_utc, utc_offset):
    """
    Return the local time of a tweet from the utc time of the tweet
    and the utc offset of the user.
    """
    utc_dt = twitter_timestamp_to_datetime(twitter_ts_utc)
    local_tz_offset = datetime.timedelta(seconds=utc_offset)
    local_dt = utc_dt + local_tz_offset
    return local_dt.isoformat()

@outputSchema('hour_block:chararray')
def hour_block(iso_ts):
    """
    Pick up the 2 hour-block in which the tweet was sent, e.g.
    0-1, 2-3, ..., 22-23
    """
    iso_dt = iso_timestamp_to_datetime(iso_ts)
    hour = iso_dt.hour

    range_length = 2
    range_bottom = hour - (hour % range_length)
    range_top = (range_bottom + range_length) % 24
    return '%02d - %02d' % (range_bottom, range_top)

def iso_timestamp_to_datetime(iso_ts):
    return datetime.datetime.strptime(iso_ts, "%Y-%m-%dT%H:%M:%S")

def twitter_timestamp_to_datetime(twitter_ts):
    """
    Create a datetime object from the twitter timestamp.
    """
    # #Tue Apr 26 08:57:55 +0000 2011
    time_obj = time.strptime(twitter_ts, "%a %b %d %H:%M:%S +0000 %Y")
    return datetime.datetime.fromtimestamp(time.mktime(time_obj))
