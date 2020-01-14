import os

filenames=[]
number_of_months = 3
output = 'TWEETS.txt'

for i in range(0,number_of_months):
    filenames.append('Tweets'+str(i)+'.txt')

with open(output, 'w') as outfile:
     outfile.write('username;date;retweets;favorites;text;geo;mentions;hashtags;id;permalink\n')
     for fname in filenames:
         with open(fname) as infile:
              for line in infile:
                  if 'username;date;retweets;favorites;text;geo;mentions;hashtags;id;permalink' not in line:
                     outfile.write(line)

