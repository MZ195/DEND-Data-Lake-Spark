import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", config['KEYS']['AWS_ACCESS_KEY_ID'])
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", config['KEYS']['AWS_SECRET_ACCESS_KEY'])
    return spark


def process_song_data(spark, input_data, output_data):
    """Loading song data from S3 bucket, transform it, then save it as parquet files.
    Keyword arguments:
    spark        -- Spark session object
    input_data   -- path to S3 input bucket
    output_data  -- path to S3 output bucket
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # Droping songs in-case of duplicates
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')
    
    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']

    # Droping artists in-case of duplicates
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """Loading log data from S3 bucket, transform it, then save it as parquet files.
    Keyword arguments:
    spark        -- Spark session object
    input_data   -- path to S3 input bucket
    output_data  -- path to S3 output bucket
    """
    # get filepath to log data file
    log_data = log_data = input_data + "log_data/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # Dropping users in-case of duplicates
    users_table = users_table.drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    
    df = df.withColumn('start_date', get_timestamp(df.ts))
    
    # Adding month to df for later use
    df = df.withColumn('month', month(df.start_date))
    
    # extract columns to create time table
    time_table = df.select('start_date',
                           hour('start_date').alias('hour'),
                           dayofmonth('start_date').alias('day'),
                           weekofyear('start_date').alias('week'),
                           month('start_date').alias('month'),
                           year('start_date').alias('year'),
                           dayofweek('start_date').alias('weekday')
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + '/songs')
    df = df.join(songs_df, (df.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df['start_date', 'userId', 'level', 'song_id', 'artist_id', 'location', 'userAgent', 'year', 'month']
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplay/')


def main():
    spark = create_spark_session()
    input_data = 's3a://'
    output_data = 's3a://'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    