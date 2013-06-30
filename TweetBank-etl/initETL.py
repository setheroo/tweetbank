## TweetBank-ETL v1.0
## Mine tweets of a particular category.  
## Align content with respective api / json function.
## see ./Humans.txt
from twython import Twython; #get tweets
import string, json, pprint; #string stuff
import re #string stuff
import urllib; #get real url
import urlparse; #parse urls
import string, os, sys, subprocess, time #string and datetime stuff
from datetime import timedelta #datetime stuff
from datetime import date #datetime stuff
from time import * #datetime stuff
import pymysql #db stuff
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='wt', db='twitterETL');
cur = conn.cursor();
harvest_list = ['LOTG','LOTG2013','myconcerts','festivals','rock','coachella','lollapalooza','bonnaroo','firefly','electricforest','campbisco','tomorrowworld','EDC','CMA','voodoofest','rockontherange','electriczoo','governorsball','mountainoasis','sonicbloom','freedomofexpression','bellaterra']
cur.execute("select max(isnull(batchid)) from TweetLog")
batch_id_cur = cur.fetchall()
batch_id = batch_id_cur[0][0]
for tweet_keyword in harvest_list:
        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""") #clear staging table
        conn.commit() 
        twitter = Twython('TPP5N6kziVqRz1olQGwGCg', '4PkiztGG5fQ27XiAAJ8w5eti6se0H8xoifaAIZMh5E', '17905874-9eWghDNYNwAFbHRWCaDHRr5d62M1Y5DLP6agk7yDI', 'NEwPdAicDKR76lGeAAKtKthuRtgI91hR2tjmi5H3M0')
        search_results = twitter.search(q=tweet_keyword, count="100")         # our search for the current keyword
        for tweet in search_results['statuses']:
                #print "        Tweet from @%s Date: %s" % (tweet['user']['screen_name'].encode('utf-8'),tweet['created_at'])
                #print "        ",tweet['text'].encode('utf-8'),"\n"
                try:	
				urlarray = re.findall(r'(?P<url>https?://[^\s]+)', tweet['text'].encode('utf-8'));
				url1 = ', '.join(urlarray);
				if len(url1) > 0: url2 = urllib.urlopen(url1).geturl();
				else: url2 = '';
				insertsql = """insert into TweetBankTemp (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, geo, url, durl) values ('"""+str(tweet['id_str'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str('1/1/2013 12:00:00 AM')+"""','"""+str(tweet_keyword)+"""','"""+str(tweet['text'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str(tweet['user']['screen_name'].encode('utf-8').replace("'","''").replace(';',''))+"""','"""+str('universal')+"""','"""+str('universal')+"""','"""+url1+"""','"""+url2+"""') """
				#print insertsql
				cur.execute(insertsql);
				conn.commit();
                except:
                        print "############### Unexpected error:", sys.exc_info()[0], "##################################";
        cur.execute("""insert into TweetBank (tweet_id, tweet_datetime, tweet_keyword, tweet, tweeter, lang, geo, url, durl) select * from TweetBankTemp where tweet_id NOT in (select distinct tweet_id from TweetBank)""");
        cur.execute("""delete from TweetBankTemp where tweet_keyword = '"""+str(tweet_keyword)+"""'""");
        conn.commit();
