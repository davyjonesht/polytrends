import sys, wc, yaml
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession
from pyspark.sql.functions import  split, explode, lit, avg

with open("../config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

CASSANDRA_CLUSTER_URL = cfg['cassandra']['url']
CASSANDRA_KEYSPACE =  cfg['cassandra']['keyspace']
CASSANDRA_TABLENAME =  cfg['cassandra']['tablename']
OUTPUT_DIR = cfg['output']['dir']

cluster_seeds = [CASSANDRA_CLUSTER_URL]#['127.0.0.1']
spark = SparkSession.builder.appName('Spark Cassandra Reader') \
	.config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4'
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext



def hashtag_wordcloud(hashtags, output):
    """Finds the frequency for each hashtag which is converted to a dictionary using a Pandas dataframe. 
	Uses a python functions 'create_hashtag_wordcloud' to plot the word cloud
	
	Args:
		hashtags (DataFrame): A DataFrame with a column containing all the hashtags in the dataset
        output (String) : A string with the name of the output directory
	Returns:
		NoneType
	"""
    
    filename = 'hashtags_word_cloud.jpeg'
    output='assets'
    hashtags = hashtags.select(explode(split(hashtags['hashtags'], ' ')))
    hashtags_freq = hashtags.groupBy(hashtags['col']).count().orderBy("count", ascending=False)
    hashtags_freq = hashtags_freq.withColumnRenamed('col', 'hashtag')
    hashtags_freq_dict = hashtags_freq.toPandas().set_index('hashtag').T.to_dict('list')
    wc.create_hashtag_wordcloud(hashtags_freq_dict, filename, output)

def scheer_wordcloud(hashtags, output):
    """Finds the frequency for each hashtag which is converted to a dictionary using a Pandas dataframe. 
	Uses a python functions 'create_hashtag_wordcloud' to plot the word cloud
	
	Args:
		hashtags (DataFrame): A DataFrame with a column containing all the hashtags in the dataset
        output (String) : A string with the name of the output directory
	Returns:
		NoneType
	"""
    
    filename = 'scheer_word_cloud.jpeg'
    output='assets'
    hashtags = hashtags.select(explode(split(hashtags['hashtags'], ' ')))
    hashtags = hashtags.filter(hashtags['col'].contains('scheer'))
	
    hashtags_freq = hashtags.groupBy(hashtags['col']).count().orderBy("count", ascending=False)
    hashtags_freq = hashtags_freq.withColumnRenamed('col', 'hashtags')
    hashtags_freq_dict = hashtags_freq.toPandas().set_index('hashtags').T.to_dict('list')
    wc.create_hashtag_wordcloud(hashtags_freq_dict, filename, output)

def plot_sentiment_for_party(df, party, output):
    """ Creates the data to plot sentiments for a particular party. The output is saved as a csv file
    with format date, sentiment, count
    Args:
        df (DataFrame): A DataFrame with columns date, party, sentiment_category.
        party (String): A String variable to identify party.
                        'L'-Liberals
                        'C'-CPC
                        'N'-NDP
    
    Returns:
        NoneType
    """
    df = df.filter(df['party']==party)
    df = df.groupBy(df['date'], df['sentiment_category']).count().orderBy("date")
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output+'/'+party+'-for-dash')
	

def plot_count_for_party(df, output):
    """ Creates the data to plot tweet counts particular party. The output is saved as a csv file
    with format date, party, count
    Args:
        df (DataFrame): A DataFrame with columns date, party
    Returns:
        NoneType
    """
    df = df.groupBy(df['date'], df['party']).count().orderBy("date")
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output+'/Tweet-Count-for-dash')
	
    
def plot_avg_sentiment_for_party(df, output):
    """ Creates the data to plot average sentiment per party. The output is saved as a csv file
    with format date, party, average_sentiment
    Args:
        df (DataFrame): A DataFrame with columns date, party, sentiment
    Returns:
        NoneType
    """
    df = df.groupBy(df['date'], df['party']).agg(avg(df['sentiment']).alias('average')).orderBy("date")
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output+'/Avg-Senti-for-dash')
	

def plot_issues_by_month(df_with_sentiment, output):
    """ Creates the data to plot visusalizations for popular issues per month. The output is saved as a csv file
    with format month, year, party, count
    Args:
        df (DataFrame): A DataFrame with columns date, party, hashtags
    Returns:
        NoneType
    """
    
    df_with_sentiment.createOrReplaceTempView("data")
    climatedata = spark.sql(" select date_format(date, 'MMM') as month, date_format(date, 'YYYY') as year, 'climate' as issue,  count(*) as count from data where \
							hashtags like '%climate%' group by year, month, issue")
    healthdata = spark.sql(" select date_format(date, 'MMM') as month, date_format(date, 'YYYY') as year, 'health' as issue,  count(*) as count from data where \
							hashtags like '%health%' group by year, month, issue")
    gundata = spark.sql(" select date_format(date, 'MMM') as month, date_format(date, 'YYYY') as year, 'gun' as issue,  count(*) as count from data where \
							hashtags like '%gun%' group by year, month, issue")
    corruptiondata = spark.sql(" select date_format(date, 'MMM') as month, date_format(date, 'YYYY') as year, 'corruption' as issue,  count(*) as count from data where \
							hashtags like '%corruption%' group by year, month, issue")
    data = climatedata.union(healthdata).union(gundata).union(corruptiondata).orderBy(['year','month'])
    data.coalesce(1).write.option("header", "true").mode("overwrite").csv(output+'/issues-for-dash')

def main():
    df = spark.read.format("org.apache.spark.sql.cassandra")\
	.options(table='datastore', keyspace='data').load()
    
    
    hashtag_wordcloud(df.select('hashtags'), OUTPUT_DIR)
    scheer_wordcloud(df.select('hashtags'), OUTPUT_DIR)
    df_with_sentiment = df.filter(df['party']!=lit('None'))
    plot_sentiment_for_party(df_with_sentiment, 'L', OUTPUT_DIR)
    plot_sentiment_for_party(df_with_sentiment, 'C', OUTPUT_DIR)
    plot_sentiment_for_party(df_with_sentiment, 'N', OUTPUT_DIR)
    plot_count_for_party(df_with_sentiment.select('date', 'party'), OUTPUT_DIR)
    plot_avg_sentiment_for_party(df_with_sentiment.select('date', 'party', 'sentiment'), OUTPUT_DIR)
    plot_issues_by_month(df_with_sentiment, OUTPUT_DIR)
    

if __name__ == '__main__':
	main()