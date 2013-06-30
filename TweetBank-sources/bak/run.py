from twython import Twython
import string, json, pprint
import urllib
from datetime import timedelta
from datetime import date
from time import *
import string, os, sys, subprocess, time
import urlparse;
# probably wasteful imports, but in this case, who cares?
import pymysql
import re

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='wt', db='TwitterETL')
cur = conn.cursor()
# connect to our database and create a cursor to do some work
#harvest_list = ['I want']
harvest_list = ['myconcerts','music','edm','hiphop','rap','nowplaying','festivals','livemusic','festivals','rock','coachella','lollapalooza','bonnaroo','metal','rockandroll','livemusic','firefly','electricforest','campbisco','tomorrowworld','EDC','CMA','voodoofest','rockontherange','electriczoo','governorsball','mountainoasis','sonicbloom','freedomofexpression','bellaterra','ultra']
## my aforementioned harvest list, use as many as you want,
## they will all be separated in the database by keyword


#cur.execute("select max(isnull(batchid)) from TweetLog")
#batch_id_cur = cur.fetchall()
#batch_id = batch_id_cur[0][0]
#print batch_id
# grabbing the last "batch id", if it exists so we
# can make log entries that make SOME sense



for tweet_keyword in harvest_list: # for each keyword, do some shit
        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""")
        conn.commit()
        # whack the temp table in case we didn't exit cleanly

        twitter = Twython('TPP5N6kziVqRz1olQGwGCg', '4PkiztGG5fQ27XiAAJ8w5eti6se0H8xoifaAIZMh5E', '17905874-9eWghDNYNwAFbHRWCaDHRr5d62M1Y5DLP6agk7yDI', 'NEwPdAicDKR76lGeAAKtKthuRtgI91hR2tjmi5H3M0')
        search_results = twitter.search(q=tweet_keyword, count="5")
        # our search for the current keyword

        #pp = pprint.PrettyPrinter(indent=3)
        # uncomment for debugging and displaying pretty output
        for tweet in search_results['statuses']:
                # some me the tweet, jerry!
                #print "        Tweet from @%s Date: %s" % (tweet['user']['screen_name'].encode('utf-8'),tweet['created_at'])
                #print "        ",tweet['text'].encode('utf-8'),"\n"
                try:	
				#url1 = re.search("(?P<url>https?://[^\s]+)", tweet['text'].encode('utf-8')).group(0);
				urlarray = re.findall(r'(?P<url>https?://[^\s]+)', tweet['text'].encode('utf-8'));
				url1 = ', '.join(urlarray);
				
				if len(url1) > 0: url2 = urllib.urlopen(url1).geturl();
				else: url2 = '';
				#print url1;
				#print re.search("(?P<url>https?://[^\s]+)", url1);
                #
                #myString = "This is my tweet check it out http://tinyurl.com/blah"
				#print re.search("(?P<url>https?://[^\s]+)", myString).group("url")
                #
                #
				insertsql = """insert into TweetBankTemp (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, geo, url, durl) values ('"""+str(tweet['id_str'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str('1/1/2013 12:00:00 AM')+"""','"""+str(tweet_keyword)+"""','"""+str(tweet['text'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str(tweet['user']['screen_name'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str('universal')+"""','"""+str('universal')+"""','"""+url1+"""','"""+url2+"""') """
				#print insertsql
				#3#insertsql = """insert into TweetBank (`tweet_id`, `tweet_datetime`, `tweet_keyword`, `tweet`, `tweeter`, `lang`, `geo`, `url`, `durl` ) select * from TweetBankTemp where tweet_id NOT in (select distinct tweet_id from TweetBank)""")
				cur.execute(insertsql)
				conn.commit()
                        # lets try to to put each tweet in our temp table for now
                        #print("""insert into TweetBankTemp (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang)
                        #                values ('"""+str(tweet['id_str'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                        #                        cast(substring('"""+str(tweet['created_at'].encode('utf-8'))+"""',5,21) as datetime),
                        #                        '"""+str('')+"""',
                        #                        '"""+str(tweet['text'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                        #                        '"""+str(tweet['user']['screen_name'].encode('utf-8').replace("'","''").replace(';',''))+"""',
                        #                        '"""+str(tweet['iso_language_code'].encode('utf-8').replace("'","''").replace(';',''))+"""'
                        #                ) """)
                except:
                        print "############### Unexpected error:", sys.exc_info()[0], "##################################"
        cur.execute("""insert into TweetBank (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, geo, url, durl) select * from TweetBankTemp where tweet_id NOT in (select distinct tweet_id from TweetBank)""")
        # take all the tweets that we DIDNT already have
        # and put them in the REAL tweet table
        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""")
        # take all THESE out of the temp table to not
        # interfere with the next keyword
		#cur.execute("""insert into TweetLog (BatchId, keyword, RunDate, HarvestedThisRun, TotalHarvested) values('"""+str(batch_id)+"""','"""+str(tweet_keyword)+"""','',((select count(*) from tweetbank where tweet_keyword = '"""+str(tweet_keyword)+"""')-(select top 1 isnull(TotalHarvested,0) from tweetlog where keyword = '"""+str(tweet_keyword)+"""' order by RunDate desc)),(select count(*) from tweetbank where tweet_keyword = '"""+str(tweet_keyword)+"""'))""")
        # add a record to the log table saying what we did!

        conn.commit()
        ## hot soup!
