import os

filenames=[]
number_of_days = 31
output = 'TweetsSept.txt'

for i in range(1,number_of_days):
    filenames.append('Tweet'+str(i)+'.txt')

with open(output, 'w') as outfile:
     outfile.write('username;date;retweets;favorites;text;geo;mentions;hashtags;id;permalink\n')
     for fname in filenames:
         with open(fname) as infile:
              for line in infile:
                  if 'username;date;retweets;favorites;text;geo;mentions;hashtags;id;permalink' not in line:
                     outfile.write(line)

