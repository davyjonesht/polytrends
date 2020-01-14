import sys, string, uuid, yaml
from textblob import TextBlob
from pyspark.sql import types
from pyspark.sql.functions import regexp_replace, trim, lower, split, array_join, lit, udf, when
from pyspark.ml.feature import StopWordsRemover


assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession

with open("../config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

TOPIC_NAME = cfg['kafka']['topic_name']
TOPIC_URL = cfg['kafka']['topic_url']
CASSANDRA_CLUSTER_URL = cfg['cassandra']['url']
CASSANDRA_KEYSPACE =  cfg['cassandra']['keyspace']
CASSANDRA_TABLENAME =  cfg['cassandra']['tablename']


cluster_seeds = [CASSANDRA_CLUSTER_URL]
spark = SparkSession.builder.appName('Spark Streaming-Kafka') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4'
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def writeToCassandra(df, epochid):
  """Writes the streaming dataframe to Cassandra.

    Args:
        df (DataFrame): A DataFrame which needs to be stored in Cassandra.
        epochid: Batch id.
    Returns:
        NoneType
  """
  df.write.format("org.apache.spark.sql.cassandra").options(table=CASSANDRA_TABLENAME, keyspace=CASSANDRA_KEYSPACE) \
    .mode('append') \
    .save()
  print("Batch updated")

def get_party(df):
    """Computes the party for a particular tweet.
       Liberal - 'L'
       CPC - 'C'
       NDP - 'N'
       Others - 'None'
    Args:
        df (DataFrame): A DataFrame with the column for which we have to compute the political party.

    Returns:
        DataFrame: A DataFrame with an extra 'party' column 
    """
    return df.withColumn('party', when(df['hashtags'].contains(lit('liberals')), lit('L')).otherwise(when(df['hashtags'].contains(lit('conservatives')), lit('C')).otherwise(when(df['hashtags'].contains(lit('ndp')), lit('N')).otherwise('None'))))


def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '[^\sa-zA-Z0-9]', '')))


def process_tweet_text(df):
    """Removes punctuation, stop words from inputCol and the output is in the outputCol Column.

    Args:
        df (DataFrame): A DataFrame with the column from which Stop Words need to be removed.

    Returns:
        DataFrame: Applying StopWordsRemover with text_clean as the input column and filtered as the output column.
    """
    df = df.withColumn('text', split(removePunctuation(df['text']), ' ').alias('text'))
    stopWordList = list(string.punctuation) + ['http', 'https', 'rt','via','...','…','’','—','—:','“'] + StopWordsRemover.loadDefaultStopWords('english')
    remover = StopWordsRemover(inputCol="text", outputCol="filtered", stopWords = stopWordList)
    df = remover.transform(df)
    df = df.withColumn('tweet', array_join(df['filtered'], ' '))
    return df.select('date', 'tweet', 'hashtags')


@udf(returnType=types.FloatType())
def sentiment(x):
    """Calculates sentiment of the string passed.

    Args:
        x (String): Text for which we have to calculate the sentiment.

    Returns:
        Float: A float value between -1 and 1. 
        -1 indicates negative sentiment.
         0 indicates neutral sentiment.
        +1 indicates positive sentiment. 
    """
    sentiment = TextBlob(x)
    return sentiment.sentiment.polarity

def get_sentiment(df):
    """Calculates the sentiment of the tweet from tweet and the output is in the sentiment Column.

    Args:
        df (DataFrame): A DataFrame with the column for which we have to calculate the stopwords.

    Returns:
        DataFrame: A DataFrame with an extra 'sentiment' column.
    """
    df = df.withColumn('sentiment' , sentiment(df['tweet']))
    df = df.withColumn('sentiment_category' , when(df['sentiment']>0, lit('positive')).otherwise(when(df['sentiment']<0, lit('negative')).otherwise(lit('neutral'))))
    return df


def main():
    
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', TOPIC_URL) \
        .option('subscribe', TOPIC_NAME).load()
    data = messages.select(messages['value'].cast('string').alias('data'))
    data.createOrReplaceTempView("tweets")
   
    ''' Pre-processing of tweets '''
    
    tweets = spark.sql(" select a[0] as username, cast(a[1] as date) as date, \
                       a[2] as retweets, a[3] as favourites, a[4] as text, a[5] as geo, a[6] as mentions, \
                       a[7] as hashtags, a[8] as id, a[9] as permalink from \
                       (select split(data, '\u0001') as a from tweets) t ")
    
    df = tweets.select('date', 'text', 'hashtags')
    df = df.filter(df['text'].isNotNull())
    df = df.filter(df['hashtags'].isNotNull())
    df = df.withColumn('hashtags', lower(df['hashtags']))
    df = process_tweet_text(df)    
    df = get_sentiment(df)
    df = get_party(df)
    
    uuid_calculator= udf(lambda : str(uuid.uuid4()), types.StringType())
    df = df.withColumn('uuid', uuid_calculator())
    
    
    ''' Writing data to Cassandra '''
    stream = df.writeStream.foreachBatch(writeToCassandra) \
        .outputMode('append').start()
       
    stream.awaitTermination()




if __name__ == '__main__':
    main()