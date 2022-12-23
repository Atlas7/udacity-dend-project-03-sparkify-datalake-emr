# Sparkify Data Lake ETL (edition: 20220911t113300)

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test your database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare your results with their expected results.

For the purpose of this exercise our chosen AWS region is us-west-2.

## Instruction to Build the Sparkify Data Lake

Follow the following steps to reproduce result.

### Step 0: populate 

Ensure you provide your AWS Access Key ID and Secret Key in `dl.cfg`. Do not surround the strings with quotes (as quotes can cause problems here).

```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

(Note: in production environment, we would normally store these details in a more secure location, rather than at the same directory as the python script / working directory)

### Step 1: visualise where the input files seat:

This step is purely for understanding. To view the public S3 bucket `udacity-dend` (where the input song and log files are stored) via S3 console, open in browser:

https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects#

See `song-data` and `log-data` for the input JSON files.


### Step 2: Create Sparkify DataLake Bucket

Go to S3 console: https://s3.console.aws.amazon.com/s3/buckets?region=us-west-2

Create Bucket.

* Bucket Name (whatever name that is unique): `sparkify-dl-20220911t113300`
* AWS Region: `us-west-2`
* Object Ownership (use default): ACLs disabled (Recommended)
* Block Public Access settings for this bucket (use default): checked (block all public access)
* Bucket versioning (use default): disable
* Tag (use default): (ignore)
* Server-side encryption (use default): Disable
* Object Lock (use default): Disable

### Step 3: Run ETL script

Run `python etl.py` via one of the following two options:

**Option 1: run locally via Udacity Workspace environment**. The processes that involve reading-from and writing-to S3 bucket may take longer time, as there is a longer distance for data to tranvel between S3 location and the Udacity workspace server. The initial development uses the Udacity workspace environment (by restricting only reading small amount of files. e.g. read just one log file: `s3a://udacity-dend/log_data/2018/11/2018-11-12-events.json` and small number of song files: `s3a://udacity-dend/song-data/*/*/*/*.json`). (to learn how the )

**Option 2: run on the AWS EMR cluster**. Launch a AWS EMR Cluster as per instructions in Appendix 2.

### Step 4: Confirm files are created on S3 bucket

Once the Python ETL script has completed running, we should see the following datasets (stored in their own folder):

#### Staging Table

1. `staing_songs` - effectively a dataset populated from the raw `song-data` JSON files. This is to make downstream investigation and development work easier.

2. `staging_events` - effectively a dataset populated from the raw `log-data` JSON files. This is to make downstream investigation and development work easier.

#### Fact Table

3. `songplays` - records in log data associated with song plays i.e. records with page NextSong. Fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

4. users - users in the app. Fields: user_id, first_name, last_name, gender, level

5. songs - songs in music database. Fields: song_id, title, artist_id, year, duration

6. artists - artists in music database. Fields: artist_id, name, location, lattitude, longitude

7. time - timestamps of records in songplays broken down into specific units. start_time, hour, day, week, month, year, weekday

### Step 5: (optional) Run basic analytical queries against the Sparkify Data Lake

Due to time constraint we have not done this yet. But if we are to do this, we can read the parquet datasets using spark and do some basic analytics - such as table joins and aggregations.


## Appendix 1 - View udacity-dend S3 bucket

To view the public S3 bucket `udacity-dend` via S3 console, open in browser:

https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects#

To list XML of the bucket:

https://udacity-dend.s3.us-west-2.amazonaws.com/

To view a log-data JSON file. e.g.

https://udacity-dend.s3.us-west-2.amazonaws.com/log-data/2018/11/2018-11-01-events.json


To view a song-data JSON file. e.g.

https://udacity-dend.s3.us-west-2.amazonaws.com/song-data/A/A/A/TRAAAAK128F9318786.json

## Appendix 2 - Launch AWS EMR Cluster

The following steps are provided by Udacity.

### Step 1: Create EMR Cluster

Go to the Amazon EMR Console.

Select "Clusters" in the menu and click the "Create cluster" button.

* Cluster: (something meaningful. In our case: `sparkify-emr-20220911t113300`)
* Release: `emr-5.20.0` (NOTE: stick with this version exact. I tried `emr-5.33` it gives error: `Cluster does not have Jupyter Enterprise Gateway application installed`. Udacity forum suggests sticking with `emr-5.20.0` resolves this issue)
* Applications: `Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0`
* Instance type: m3.xlarge
* Number of instance: 3
* EC2 key pair: Proceed without an EC2 key pair or feel free to use one if you'd like

Keep the remaining default setting and click "Create cluster" on the bottom right.

It may take some time for the cluster to spin up.

### Step 2: Wait for Cluster "Waiting" Status

Once you create the cluster, you'll see a status next to your cluster name that says Starting. Wait a short time for this status to change to Waiting before moving on to the next step.

### Step 3: Create Notebook

Now that you launched your cluster successfully, let's create a notebook to run Spark on that cluster.

Select "Notebooks" in the menu on the left, and click the "Create notebook" button.


### Step 4: Configure your notebook

Enter a name for your notebook

Select "Choose an existing cluster" and choose the cluster you just created

Use the default setting for "AWS service role" - this should be "EMR_Notebooks_DefaultRole" or "Create default role" if you haven't done this before. Note: if you use "Create default Role" it may return an error message. Ignore that error and click create notebook - it should go through.

You can keep the remaining default settings and click "Create notebook" on the bottom right.

### Step 5: Wait for Notebook "Ready" Status, Then Open

Once you create an EMR notebook, you'll need to wait a short time before the notebook status changes from Starting or Pending to Ready. Once your notebook status is Ready, click the "Open" button to open the notebook.

### Step 6: Start Coding!

Once Notebook is opened, change the Kernal (Kernal -> Change Kernal -> PySpark)


Open Question: do we need to update this part of the code (the `.config()`) part). (Answer: probably not)

```python
def create_spark_session():
    """
    create a spark session
    """ 
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
```




## Useful readings

https://stackoverflow.com/questions/33356041/technically-what-is-the-difference-between-s3n-s3a-and-s3

Create S3 Bucket: 

* https://knowledge.udacity.com/questions/762997
* https://knowledge.udacity.com/questions/762290
* https://knowledge.udacity.com/questions/121606
* https://knowledge.udacity.com/questions/101153 - implies we don't need to make S3 bucket public (as we are going to delete it at the end of exercise anyway
* https://knowledge.udacity.com/questions/132987 - when write to S3 data-lake bucket, make sure we use correct version in spark config. Use `s3a` instead of `s3n`.