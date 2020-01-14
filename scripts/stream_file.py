import os, sys, yaml
from kafka import KafkaProducer

with open("../config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

TOPIC_NAME = cfg['kafka']['topic_name']
TOPIC_URL = cfg['kafka']['topic_url']

def main():
   """ Filepath represents the relative path
       of the file to be streamed.
   """
   filepath = sys.argv[1]
   
   if not os.path.isfile(filepath):
       print("File path {} does not exist. Exiting...".format(filepath))
       sys.exit()

   producer = KafkaProducer(bootstrap_servers=[TOPIC_URL])
   with open(filepath) as fp:
       cnt = 0
       for line in fp:
           print("line {} contents {}".format(cnt, line))
           producer.send(TOPIC_NAME, line.encode('utf-8'))
           cnt += 1
   print("End of stream")
  


if __name__ == '__main__':
   main()

