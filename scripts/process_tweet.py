import sys
import re
import wc
import plot_graph
import string
from textblob import TextBlob
import pandas as pd
import datetime

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import regexp_replace, trim, lower, split, col, explode, array_join, lit, udf, when, avg
from pyspark.ml.feature import StopWordsRemover
spark = SparkSession.builder.appName('reddit_average_df').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

    
def get_party(df):
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



def main(inputs, output):
    '''define the schema'''
    tweets_schema = types.StructType([
		    types.StructField('username', types.StringType()),
		    types.StructField('date', types.DateType()),
		    types.StructField('retweets', types.StringType()),
		    types.StructField('favorites', types.StringType()),
		    types.StructField('text', types.StringType()),
		    types.StructField('geo', types.StringType()),
		    types.StructField('mentions', types.StringType()),
		    types.StructField('hashtags', types.StringType()),
		    types.StructField('id', types.StringType()),
		    types.StructField('permalink', types.StringType())
		])
    '''
    pass the schema when reading input file to avoid Spark DataFrames from directly infering the Schema from the input
    '''
    df = spark.read.format("csv").option('header','true').option('delimiter','\u0001').schema(tweets_schema).load(inputs)
    df = df.select('date', 'text', 'hashtags')
    ''' start preprocessing '''
    df = df.filter(df['text'].isNotNull())
    df = df.filter(df['hashtags'].isNotNull())
    df = df.withColumn('hashtags', lower(df['hashtags']))
    df = process_tweet_text(df)    
    df = get_sentiment(df)
    df = get_party(df)
    df.show()
 


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


