import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    create a spark session
    """ 
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


@udf(TimestampType())
def get_timestamp(unix_epoch):
    """
    Convert UNIX epoc time (in milliseconds) to seconds, then to python datetime
    """
    return datetime.fromtimestamp(unix_epoch/1000)


def build_songs_table(df):
    """
    Given a song-data spark dataframe, create the songs view.
    """
    return df.select(
        col("song_id").alias("song_id"),
        col("title").alias("title"),
        col("artist_id").alias("artist_id"),
        col("year").alias("year"),
        col("duration").alias("duration")
    )


def build_artists_table(df):
    """
    Given a song-data spark dataframe, create the artists view.
    """
    return df.select(
        col("artist_id").alias("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
    ).drop_duplicates()


def build_users_table(df):
    """
    Given a log-data spark dataframe, create the users view.
    """
    return df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender").alias("gender"),
        col("level").alias("level")
    ).drop_duplicates()


def build_time_table(df, timestamp_field="timestamp"):
    """
    Given a log-data spark dataframe, create the table view.
    
    Useful read: https://sparkbyexamples.com/spark/spark-extract-hour-minute-and-second-from-timestamp/#:~:text=Solution%3A%20Spark%20functions%20provides%20hour,string%20column%20containing%20a%20timestamp.
    """
    return df\
        .select(col(timestamp_field))\
        .drop_duplicates()\
        .withColumn("hour", hour(col(timestamp_field)))\
        .withColumn("day", dayofmonth(col(timestamp_field)))\
        .withColumn("week", weekofyear(col(timestamp_field)))\
        .withColumn("month", month(col(timestamp_field)))\
        .withColumn("year", year(col(timestamp_field)))\
        .withColumn("weekday", dayofweek(col(timestamp_field)))


def write_partitioned_parquet_to_datalake(df, outdir, mode="overwrite", partition_by=None):
    """
    Write a spark dataframe as a partitioned parquet binary to an output directory.
    """
    df\
        .write\
        .partitionBy(*partition_by)\
        .mode('overwrite')\
        .parquet(outdir)

    
def write_unpartitioned_parquet_to_datalake(df, outdir, mode="overwrite"):
    """
    Write a spark dataframe as an unpartitioned parquet binary to an output directory.
    """   
    df\
        .write\
        .mode(mode)\
        .parquet(outdir)

    
def process_song_data(spark, input_data, output_data):
    """
    proceess song-data
    """
    
    # get filepath to song data file
    # https://knowledge.udacity.com/questions/111283
    #song_data = os.path.join(input_data, "song-data/A/A/A/TRAAAAK128F9318786.json")
    #song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")
    
    # read song data file
    # https://knowledge.udacity.com/questions/111283
    df = spark.read.json(song_data, encoding='UTF-8')

    # write staging_songs table to parquet files
    write_unpartitioned_parquet_to_datalake(
        df=df,
        outdir=os.path.join(output_data, "staging_songs/staging_songs.parquet"),
        mode="overwrite"
    )  
    
    
    # extract columns to create songs table
    songs_table = build_songs_table(df)
    
    # write songs table to parquet files partitioned by year and artist
    # https://knowledge.udacity.com/questions/399511
    write_partitioned_parquet_to_datalake(
        df=songs_table,
        outdir=os.path.join(output_data, "songs/songs.parquet"),
        mode="overwrite",
        partition_by=["year", "artist_id"]
    )

    # extract columns to create artists table
    artists_table = build_artists_table(df)
    
    # write artists table to parquet files
    write_unpartitioned_parquet_to_datalake(
        df=songs_table,
        outdir=os.path.join(output_data, "artists/artists.parquet"),
        mode="overwrite"
    )


def process_log_data(spark, input_data, output_data):
    """
    proceess log-data
    """
    
    # get filepath to log data file
    #log_data = os.path.join(input_data, "log_data/2018/11/2018-11-12-events.json")
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data, encoding='UTF-8')
 
    # write staging_events table to parquet files
    write_unpartitioned_parquet_to_datalake(
        df=df,
        outdir=os.path.join(output_data, "staging_events/staging_events.parquet"),
        mode="overwrite"
    )

    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")
    
    # extract columns for users table    
    users_table = build_users_table(df)
    
    # write users table to parquet files
    write_unpartitioned_parquet_to_datalake(
        df=users_table,
        outdir=os.path.join(output_data, "users/users.parquet"),
        mode="overwrite"
    )

    # create timestamp column from original timestamp column
    #get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = build_time_table(df, timestamp_field="start_time")

    # write time table to parquet files partitioned by year and month
    write_partitioned_parquet_to_datalake(
        df=time_table,
        outdir=os.path.join(output_data, "time/time.parquet"),
        mode="overwrite",
        partition_by=["year", "month"]
    )

    # read in staging_songs data to use for songplays table
    ss_df = spark.read.parquet(os.path.join(output_data, "staging_songs/staging_songs.parquet"))

    # extract columns from joined song and log datasets to create songplays table 
    #https://knowledge.udacity.com/questions/150979
    songplays_table = df\
        .join(
            ss_df,
            on=((df.song == ss_df.title) & (df.artist == ss_df.artist_name) & (df.length == ss_df.duration)),
            how='left_outer'
        )\
        .select(
            df["start_time"],
            df["userId"].alias('user_id'),
            df["level"],
            ss_df["song_id"],
            ss_df["artist_id"],
            df["sessionId"].alias("session_id"),
            df["location"],
            df["useragent"].alias("user_agent"),
            year(df["start_time"]).alias('year'),
            month(df["start_time"]).alias('month')
        )

    # write songplays table to parquet files partitioned by year and month
    write_partitioned_parquet_to_datalake(
        df=songplays_table,
        outdir=os.path.join(output_data, "songplay/songplay.parquet"),
        mode="overwrite",
        partition_by=["year", "month"]
    )


def main():
    """
    Populate Sparkify Data Lake
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dl-20220911t113300/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
