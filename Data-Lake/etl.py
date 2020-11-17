import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song_data files

    Reads song_data files from S3 and creates "songs" and "artists" 
    tables.
    
    Parameters
    -----------
        spark: SparkSession
        input_data: Input S3 file path 
        output_data: Path to output files
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data + "/*json")

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", 
        "artist_latitude", "artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').partitionBy("artist_id").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """Processes log_data files

    Reads log_data files from S3 and creates "time", "users" 
    and "songplays" tables.
    
    Parameters
    -----------
        spark: SparkSession
        input_data: Input S3 file path 
        output_data: Path to output files
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data"

    # read log data file
    df = spark.read.json(log_data + "/*json")
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').partitionBy("userId").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: from_unixtime(x, "dd/MM/yyyy HH:MM:SS"))
    #df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select('start_time',
        hour("start_time").alias('hour'),
        dayofmonth("start_time").alias('day'),
        month("start_time").alias('month'), 
        year("start_time").alias('year'),
        dayofweek("start_time").alias('weekday') 
        )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")

    # Create SQL views for query
    song_df.createOrReplaceTempView('song_table')
    df.createOrReplaceTempView('log_table')

    # SQL query to join tables to get song_plays
    sql_query = '''
        SELECT l.start_time, l.userId, l.level, s.song_id, s.artist_id, l.sessionId, l.location, l.userAgent
        FROM log_table l
        LEFT JOIN song_table s 
        ON l.song = s.title
       '''
    joined_table = spark.sql(sql_query)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_table.select("start_time",
                        year("start_time").alias('year'),
                        month("start_time").alias('month'),
                        "userId", "level", "song_id", "artist_id", 
                        "sessionId", "location", "userAgent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "song_play/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
