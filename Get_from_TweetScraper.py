# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import os
import getpass
import shutil
import subprocess
import shlex
import json
import glob
import sys
import os.path
import copy
import time
import ast
import pandas as pd
import pickle
import json
import csv
import numpy as np
from twython import Twython, TwythonRateLimitError, TwythonError
import logging
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
import multiprocessing
import pymysql
from sqlalchemy import create_engine
import sqlalchemy
engine9 = create_engine("mysql+pymysql://marksproject:IJKDEJFRknnkfr!!78278w2kjde@145.100.59.121/publicsphere?charset=utf8mb4")
con = engine9.connect()


def del_scraper(screen_name,password,main_dir,TweetScraper_path,saving_path,files_path):
    try:
        #Moving files to dedicated folder
        if os.path.isdir(TweetScraper_path) is True:
            print("{} Moving any files to dedicated folder {}.".format(screen_name,saving_path))
            for tweet_file in os.listdir(files_path):
                if tweet_file.startswith("{}".format(screen_name)):
                    shutil.move(tweet_file, saving_path)
            print("{} FILES MOVED TO FOLDER {}.".format(screen_name,saving_path,"/",screen_name))
            sudo = shlex.split("sudo -u")
            del_folder = shlex.split("rm -rf TweetScraper")
            # Deleting old TweepScraper files
            print("ATTN: DELETING TWEETSCRAPER FILES, PRESS ENTER TO CONTINUE.")
            print("Move tweet folder NOW if you do not want the files to be deleted. DELETE MANUALLY IF MOVING FILES.")
            yes = {'yes','y', 'ye', ''}
            no = {'no','n'}
            choice = input("Type 'YES' to delete TweetScraper files (Note: check if tweet files are saved before).").lower()
            if choice in yes:
                subprocess.Popen("sudo",shell=True,stdout=subprocess.PIPE)
                subprocess.Popen(password,shell=True,stdout=subprocess.PIPE)
                subprocess.Popen(del_folder,shell=True,stdout=subprocess.PIPE)
                shutil.rmtree(files_path, ignore_errors=True)
            elif choice in no:
                return False
            else:
                sys.stdout.write("Please respond with 'yes' or 'no'")
            
        elif os.path.isdir(TweetScraper_path) is False:
            print("No TweetScraper folder found.")
    except SyntaxError:
        pass
    return


def scraper(screen_name,password,main_dir,TweetScraper_path,saving_path,files_path,since,until):
    try:
        sudo = shlex.split("sudo -u")
        del_scraper(screen_name,password,main_dir,TweetScraper_path,saving_path,files_path)
        os.chdir(main_dir)
        print("Downloading TweepScraper in {}.".format(TweetScraper_path))
        os.system("git clone https://github.com/jonbakerfish/TweetScraper.git")
        print("Download complete. Checking TweetScraper.")
        os.chdir("TweetScraper/")
        os.system("scrapy list")
        command1 = shlex.split('scrapy crawl TweetScraper -a query="from:{}"'.format(screen_name))
        command2 = shlex.split('scrapy crawl TweetScraper -a query="to:{} since:{} --until:{}"'.format(screen_name,since,until))
        if (since == None) and (until == None):
            print("This command will be run in the terminal: ",command1)
            print("{} Getting tweets from TweetScraper. This may take some time so please wait...".format(screen_name))
            subprocess.Popen(sudo,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            subprocess.Popen(password,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            cmd = subprocess.Popen(command1,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            print(subprocess.check_output(command1))
            print("Command sent. Waiting till download is finished...")
            cmd.communicate()
        elif (since != None) and (until != None):
            print("This command will be run in the terminal: ",command2)
            print("{} Getting replies from TweetScraper. This may take some time so please wait...".format(screen_name,since,until))
            subprocess.Popen(sudo,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            subprocess.Popen(password,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            cmd = subprocess.Popen(command2,universal_newlines=True,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            print(subprocess.check_output(command1))
            print("Command sent. Waiting till download is finished...")
            cmd.communicate()
        print("Scraping complete for {}.".format(screen_name))
    except SyntaxError:
        pass
    return


def create_saving_path(screen_name,saving_path):
    if not os.path.exists(saving_path):
        print("Creating a dedicated file for {} at .".format(screen_name,saving_path))
        os.makedirs(saving_path)
    elif os.path.exists(saving_path):
        if os.listdir(saving_path):
            print("There seems to already be a dedicated folder for {} at {}. Please move files and empty folder.".format(screen_name,saving_path))
            yes = {'yes','y', 'ye', ''}
            no = {'no','n'}
            choice = input("Type 'YES' to delete existing folder. Note: Make sure tweet files are saved somewhere else.").lower()
            if choice in yes:
                del_tweets = shlex.split("rm -rf tweets")
                subprocess.Popen(del_tweets,shell=True,stdout=subprocess.PIPE)
                del_replies = shlex.split("rm -rf replies")
                subprocess.Popen(del_replies,shell=True,stdout=subprocess.PIPE)
            elif choice in no:
                sys.exit("Script will bnow be terminated.")
        elif not os.listdir(saving_path):
            pass
    return


def open_files(screen_name,ids,path):
    print("{} getting IDs from TweetScrapper.".format(screen_name))
    for file in os.listdir(path):
        ids.append(int(file))
        print("{} {} tweets gotten from TweetScrapper.".format(screen_name,len(ids)))
    return ids


def get_sql(screen_name):
    data = pd.read_sql_query("SELECT id_str, created_at FROM Twitter_Posts WHERE twitter_name = '"+screen_name+"'" , con=con)
    df_tweet =  pd.DataFrame(data,columns=['id_str','created_at'])
    df_tweet['created_at'] = pd.to_datetime(df_tweet['created_at']).dt.date
    created_at = df_tweet['created_at'].tolist()
    since = min(created_at)
    until = max(created_at)
    alltweets = df_tweet['id_str'].tolist()
    print("{} getting {} ids from SQL as alltweets.".format(screen_name,len(alltweets)))
    print("Since: ",since)
    print("Until: ",until)
    return alltweets,since,until


def get_ids(screen_name,alltweets,ids,len_ids):
    for d in alltweets:
        d['id']=int(d['id'])
        for _id in ids:
            if _id == d['id']:
                ids.remove(_id)
                new_len_ids = len(ids)
                removed = len_ids - new_len_ids
                print("{} IDs removed: {}".format(screen_name,removed))
    return ids


#getting replies
def filter_replies(screen_name,alltweets,replytweets,final_replytweets,counter,big_counter):
    counter += 1
    big_counter += 1
    left = len(replytweets) - len(final_replytweets)
    for tweet in alltweets:
        for reply in replytweets:
            for key,value in reply.items():
                if 'in_reply_to_status_id' in key:
                    if (value is not None) and (value == tweet):
                        final_replytweets.append(reply)
                        print("{} {} directed comment tweets downloaded so far.".format(screen_name,len(final_replytweets)))
                        if counter == 100:
                            print("{} directed tweets left to finish: {}".format(screen_name,left))
                        if counter == 10:
                            print("{} counter reached {}, saving tweets.".format(screen_name,big_counter))
                            with open(screen_name+'_final_replytweets.json', 'w') as f:
                                json.dump(final_replytweets, f)
                            print("{} {} TWEETS SAVED.".format(screen_name,len(final_replytweets)))
                            counter = 0
    return final_replytweets


#add administrative columns
def add_cols(x):
    add_cols = []
    for i in x:
        if 'extended_entities' in i:
            if i['extended_entities']['media'][0]['type'] == 'video':
                add_cols.append([i['extended_entities']['media'][0]['type'],i['user']['id'],i['id']])
            elif i['extended_entities']['media'][0]['type'] != 'video':
                add_cols.append(["not video",i['user']['id'],i['id']])
        elif 'extended_entities' not in i:
            add_cols.append(["not video",i['user']['id'],i['id']])

    return add_cols


#flatten alltweets
def flatten(x):
    d = copy.deepcopy(x)
    for key in list(d):
        if isinstance(d[key], list):
            value = d.pop(key)
            for i, v in enumerate(value):
                d.update(flatten({'{}_{}'.format(key, i): v}))
        elif isinstance(d[key], dict):
            d[key] = str(d[key])
    return d


def genre(df_tweet):
    op_news = ["AC360","TuckerCarlson","hardball"]
    news = ["CBSEveningNews","11thHour","NewsHour","ABCWorldNews","NightLine","FaceTheNation","60Minutes","NBCNews","MeetThePress","NOS","nosop3","RTLnieuws","Nieuwsuur"]
    parody = ["SouthPark","nbcsnl","TheOnion"]
    if df_tweet["twitter_name"] in op_news:
        return "Opinionated news"
    if df_tweet["twitter_name"] in news:
        return "News"
    if df_tweet["twitter_name"] in parody:
        return "Parody"
    else:
        return "News satire"


def write_df(screen_name,alltweets):
    
    if len(alltweets) > 0:
        #write DataFrame
        print("{} Flatten df.".format(screen_name))
        df_tweet_flat = pd.DataFrame([flatten(tweet) for tweet in alltweets])
        df_tweet_flat = df_tweet_flat.drop_duplicates(subset="id")

        #add_cols
        print("{} Adding columns.".format(screen_name))
        add_tweet_cols = add_cols(alltweets)
        df_add_tweet_cols = pd.DataFrame(add_tweet_cols, columns = ["video","user_id","id"])
        df_tweet = pd.merge(df_add_tweet_cols.set_index("id"),df_tweet_flat.set_index("id"), right_index=True, left_index=True).reset_index()
        print("{} Fixing date.".format(screen_name))
        df_tweet['created_at'] = pd.to_datetime(df_tweet['created_at']).dt.date
        df_tweet.insert(0, "platform", "Twitter")
        df_tweet.insert(1, "twitter_name", screen_name)
        df_tweet["rowNumber"] = np.arange(len(df_tweet))
        print("{} Adding genre.".format(screen_name))
        df_tweet["genre"] = df_tweet.apply(genre, axis=1)
        cols = list(df_tweet)
        print("{} Adding rowNumber.".format(screen_name))
        cols.insert(0, cols.pop(cols.index("rowNumber")))
        cols.insert(3, cols.pop(cols.index("genre")))
        df_tweet = df_tweet.loc[:, cols]
        print("{} Clearing Duplicates.".format(screen_name))
        df_tweet = df_tweet.drop_duplicates(subset="id")
        #df_tweet = df_tweet.loc[df_tweet.astype(str).drop_duplicates().index]
    
    elif len(alltweets) == 0:
        print("No tweets in list.")
        df_tweet = pd.DataFrame()
        pass
    return df_tweet


# + code_folding=[]
def get_all(screen_name):
    
    print("To begin, please enter the admin password for this machine.")
    password = getpass.getpass()
    
    ###### Path where scraped Tweetscraper and tweet files will be saved ###### 
    
    yes = {'yes','y', 'ye', ''}
    no = {'no','n'}

    choice = input("Would you like to save files in a new location?").lower()
    if choice in yes:
        #Path to choosen directory
        main_dir = input('Enter path to main directory where TweetScraper will be downloaded, e.g. /Users/home')
        saving_dir = input('Enter path to download directory where tweets and replies will be downloaded, e.g. /Users/home')
        
    elif choice in no:
        #Path to current directory
        main_dir = os.getcwd()
        saving_dir = os.getcwd()
        print("Saving files for {} in current directory at {}".format(screen_name,main_dir))
    
    else:
        sys.stdout.write("Please respond with 'yes' or 'no'")
    
    #Path to TweetScraper
    print("This is the directory where TweetScraper will be saved: {}".format(main_dir))
    files_path = '{}/TweetScraper/'.format(main_dir)
    TweetScraper_path = '{}/TweetScraper/Data/tweet/'.format(main_dir)
    
    #Path where ONLY tweet files will be saved
    saving_path_tweets = '{}/{}/tweets/'.format(saving_dir,screen_name)
    print("This is the directory where ONLY tweets will be saved: {}".format(saving_path_tweets))
    create_saving_path(screen_name,saving_path_tweets)

    #Path where ONLY reply files will be saved
    saving_path_replies = '{}/{}/replies/'.format(saving_dir,screen_name)
    print("This is the directory where ONLY replies will be saved: {}".format(saving_path_replies))
    create_saving_path(screen_name,saving_path_replies)
    
    ############################################################################################
    ######################################### TWEETS ###########################################
    ############################################################################################
    if os.path.isdir(TweetScraper_path) is False:
        #scrape tweets from Tweetscraper
        print("TweepScraper not downloaded yet.")
        scraper(screen_name,password,main_dir,TweetScraper_path,saving_path_tweets,files_path,None,None)
    
    elif os.path.isdir(TweetScraper_path) is True:
        print("{} Resuming from TweetScraper folder for tweets.".format(screen_name))
    
    if os.path.isfile('{}_final_replytweets.json'.format(screen_name)) is False:
        print("Creating new final_replytweets file for {}.".format(screen_name))
        final_replytweets = []
        
        if os.path.isfile('{}_replytweets.json'.format(screen_name)) is False:
            print("Creating new replytweets file for {}.".format(screen_name))
            replytweets = []

            if os.path.isfile('{}_df_tweet.json'.format(screen_name)) is False:
                print("Creating new df_tweet for {}.".format(screen_name))

                if os.path.isfile('{}_alltweets.json'.format(screen_name)) is False:
                    print("Creating new alltweets file for {}.".format(screen_name))
                    alltweets = []

                elif os.path.isfile('{}_alltweets.json'.format(screen_name)) is True:
                    print("Resuming from old alltweets file for {}.".format(screen_name))
                    with open('{}_alltweets.json'.format(screen_name), 'r') as f:
                        alltweets = json.load(f)

                #Getting counter
                print("{} getting counter.".format(screen_name))
                counter_tweet_ids = []
                counter_tweet_ids = open_files(screen_name,counter_tweet_ids,TweetScraper_path)
                print("{} Clearing duplicate tweet IDs.".format(screen_name))
                counter_tweet_ids = list(set(counter_tweet_ids))
                len_counter_tweet_ids = len(counter_tweet_ids)
                if os.path.isfile('{}_tweet_ids.json'.format(screen_name)) is False:
                    tweet_ids = counter_tweet_ids
                    print("{} Saving tweet IDs list of length: {}".format(screen_name,len(tweet_ids)))
                    with open('{}_tweet_ids.json'.format(screen_name), 'w') as f:
                        json.dump(tweet_ids, f)

                if len(alltweets) != len_counter_tweet_ids:

                    #Get tweet IDs
                    if os.path.isfile('{}_tweet_ids.json'.format(screen_name)) is True:
                        print("Resuming from old tweet IDs file for {}.".format(screen_name))
                        with open('{}_tweet_ids.json'.format(screen_name), 'r') as f:
                            tweet_ids = json.load(f)
                        print("{} Clearing duplicate tweet IDs.".format(screen_name))
                        tweet_ids = list(set(tweet_ids))
                        print("{} Saving backup tweet IDs.".format(screen_name))
                        with open('{}_tweet_ids_backup.json'.format(screen_name), 'w') as f:
                            json.dump(tweet_ids, f)
                        print("{} Lenght of tweet IDs list: {}".format(screen_name,len(tweet_ids)))
                    elif os.path.isfile('{}_tweet_ids.json'.format(screen_name)) is False:
                        print("Creating new tweet ids file for {}.".format(screen_name))
                        print("{} Length of new tweet IDs list: {}.".format(screen_name,len(tweet_ids)))

                    #filter used ids from ids list
                    print("{} Getting tweet IDs.".format(screen_name))
                    tweet_ids = get_ids(screen_name,alltweets,tweet_ids,len_counter_tweet_ids)
                    tweet_ids = list(set(tweet_ids))
                    print("{} Savings tweet IDs.".format(screen_name))
                    with open('{}_tweet_ids.json'.format(screen_name), 'w') as f:
                        json.dump(tweet_ids, f)
                    print("{} Saving backup tweet IDs.".format(screen_name))
                    with open('{}_tweet_ids_backup.json'.format(screen_name), 'w') as f:
                        json.dump(tweet_ids, f)
                    print("{} Ajusted lenght of tweet IDs list: {}".format(screen_name,len(tweet_ids)))

                    ######################## GET TWEETS ########################

                    #Twitter API tokens
                    access_key = "1196733070210215937-kOtTpMiS1BFu7FYzEgFbkzmQKDyvGC"
                    access_secret = "mLot2xban1C0774E2N9fJAzPJWkoRT31Iik4MHQtbZMcQ"
                    consumer_key = "gMwo76vm414OWvYnkiNqyv6LE"
                    consumer_secret = "3wMHuySlzbHrSvHXG6LFZ9M6vpHXJVVJTNx92QUTU2UHcN13hh"
                    #authorize twitter, initialize tweepy
                    api = Twython(consumer_key, consumer_secret, access_key, access_secret)

                    counter = 0
                    big_counter = 0
                    tweet_ids_chunks = [tweet_ids[i:i+100] for i in range(0, len(tweet_ids), 100)]
                    for tweet_id in tweet_ids_chunks:
                        try:
                            new_tweets = api.lookup_status(id=tweet_id,include_entities=True)
                            for tweet in new_tweets:
                                alltweets.append(tweet)
                            print("{} {} tweets downloaded so far.".format(screen_name,len(alltweets)))
                            counter += 1
                            big_counter += 1
                            left = len_counter_tweet_ids - len(alltweets)
                            if big_counter == 10:
                                print("{} counter reached {}, saving tweets.".format(screen_name,big_counter))
                                print("{} {} tweet IDs left to finish".format(screen_name,left))
                                with open('{}_alltweets.json'.format(screen_name), 'w') as f:
                                    json.dump(alltweets, f)
                                with open('{}_alltweets_backup.json'.format(screen_name), 'w') as f:
                                    json.dump(alltweets, f)
                                print("{} {} TWEETS SAVED.".format(screen_name,len(alltweets)))
                            if counter == 1000:
                                print("{} counter reached {}, saving tweets.".format(screen_name,big_counter))
                                print("{} {} tweet IDs left to finish".format(screen_name,left))
                                with open('{}_alltweets.json'.format(screen_name), 'w') as f:
                                    json.dump(alltweets, f)
                                with open('{}_alltweets_backup.json'.format(screen_name), 'w') as f:
                                    json.dump(alltweets, f)
                                print("{} {} TWEETS SAVED.".format(screen_name,len(alltweets)))
                                counter = 0
                        except TwythonRateLimitError as error:
                            with open('{}_alltweets.json'.format(screen_name), 'w') as f:
                                json.dump(alltweets, f)
                            with open('{}_alltweets_backup.json'.format(screen_name), 'w') as f:
                                json.dump(alltweets, f)
                            print("{} Error {} at tweet id: {}".format(screen_name,error.error_code, alltweets[-1]['id']))
                            remainder = abs(float(api.get_lastfunction_header(header='x-rate-limit-reset')) - time.time())
                            del api
                            print("{} Resuming in {} seconds".format(screen_name,remainder))
                            time.sleep(remainder)
                            api = Twython(consumer_key, consumer_secret, access_key, access_secret)
                            logging.basicConfig(level=logging.ERROR)
                            logging.error('This error occured: {}'.format(error))
                        except TwythonError as error:
                            print("{} Error {} at tweet id: {}".format(screen_name,error.error_code, alltweets[-1]['id']))
                            for i in tweet_id:
                                tweet_ids.remove(i)
                                for file in os.listdir(path):
                                    if i == int(file):
                                        os.remove(file)
                            with open('{}_tweet_ids.json'.format(screen_name), 'w') as f:
                                json.dump(tweet_ids, f)
                            with open('{}_tweet_ids_backup.json'.format(screen_name), 'w') as f:
                                json.dump(tweet_ids, f)
                            with open('{}_alltweets_backup.json'.format(screen_name), 'w') as f:
                                json.dump(alltweets, f)
                            pass

                    #END OF ALLTWEETS COLLECTION
                    print("{} finished downloading {} alltweets. TWEETS COLLECTED. Saving files.".format(screen_name, len(alltweets)))
                    with open('{}_alltweets.json'.format(screen_name), 'w') as f:
                        json.dump(alltweets, f)
                    with open('{}_alltweets_backup.json'.format(screen_name), 'w') as f:
                        json.dump(alltweets, f)

                elif len(alltweets) == len_counter_tweet_ids:
                    print("{} already collected all {} alltweets.".format(screen_name,len(alltweets)))

                #clean output
    #             print("{} Clearing alltweets of lenght: {}".format(len(alltweets)))
    #             alltweets = [i for n, i in enumerate(alltweets) if i not in alltweets[n + 1:]]

                #write DataFrame
                print("{} Writing df_tweet.".format(screen_name))
                df_tweet = write_df(screen_name,alltweets)
                print("{} Saving df_tweet of length: {}.".format(screen_name,len(df_tweet)))
                df_tweet.to_json(screen_name+'_df_tweet.json')
                df_tweet.to_csv(screen_name+'_df_tweet.csv',index=False)
                print("{} JSON and CSV files have been saved with {} tweets. DONE!".format(screen_name,len(df_tweet)))

            elif os.path.isfile('{}_df_tweet.json'.format(screen_name)) is True:
                print("{} already collected all df_tweet.".format(screen_name))
                df_tweet = pd.read_csv('{}_df_tweet.csv'.format(screen_name))
                with open('{}_alltweets.json'.format(screen_name), 'r') as f:
                    alltweets = json.load(f)
                print("{} Length to alltweets: {}.".format(screen_name,len(alltweets)))
                print("{} Length to df_tweet: {}.".format(screen_name,len(df_tweet)))
            
            #Deleting TweetScraper tweet files
            del_scraper(screen_name,password,main_dir,TweetScraper_path,saving_path_tweets,files_path)
            
            ############################################################################################
            ######################################### REPLIES ##########################################
            ############################################################################################

            #get alltweets from SQL and scrape twitter
    #         alltweets,since,until = get_sql(screen_name)
            # Creating since and until variables
            df_tweet['created_at'] = pd.to_datetime(df_tweet['created_at']).dt.date
            created_at = df_tweet['created_at'].tolist()
    #         created_at = list(alltweets[0]['created_at'])
            since = min(created_at)
            until = max(created_at)
            print("{} Alltweets since: {}, until: {}.".format(screen_name,since,until))

            if os.path.isdir(TweetScraper_path) is False:
                #scrape tweets from Tweetscraper
                print("TweepScraper not downloaded yet.")
                scraper(screen_name,password,main_dir,TweetScraper_path,saving_path_replies,files_path,since,until)

            elif os.path.isdir(TweetScraper_path) is True:
                print("{} Resuming from TweetScraper folder for tweets.".format(screen_name))

            #Getting counter
            print("{} getting counter.".format(screen_name))
            counter_reply_ids = []
            counter_reply_ids = open_files(screen_name,counter_reply_ids,TweetScraper_path)
            print("{} Clearing duplicate reply IDs.".format(screen_name))
            counter_reply_ids = list(set(counter_reply_ids))
            len_counter_reply_ids = len(counter_reply_ids)
            if os.path.isfile('{}_reply_ids.json'.format(screen_name)) is False:
                reply_ids = counter_reply_ids
                print("{} Saving reply IDs list of length: {}".format(screen_name,len(reply_ids)))
                with open('{}_reply_ids.json'.format(screen_name), 'w') as f:
                    json.dump(reply_ids, f)
                print("{} Saving reply IDs backups".format(screen_name))
                with open('{}_reply_ids_backups.json'.format(screen_name), 'w') as f:
                    json.dump(reply_ids, f)

        ######################## GET REPLIES ########################

        elif os.path.isfile('{}_replytweets.json'.format(screen_name)) is True:
            print("Resuming from old replytweets file for {}.".format(screen_name))
            with open('{}_replytweets.json'.format(screen_name), 'r') as f:
                replytweets = json.load(f)
            print("{} Clearing duplicate replytweets.".format(screen_name))
            replytweets = [i for n, i in enumerate(replytweets) if i not in replytweets[n + 1:]]
            print("{} Saving backup replytweets.".format(screen_name))
            with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                json.dump(replytweets, f)
            print("{} Lenght of replytweets list: {}".format(screen_name,len(replytweets)))

        if len(replytweets) != len_counter_reply_ids:

            #open files and get ids
            if os.path.isfile('{}_reply_ids.json'.format(screen_name)) is True:
                print("Resuming from old reply IDs file for {}.".format(screen_name))
                with open('{}_reply_ids.json'.format(screen_name), 'r') as f:
                    reply_ids = json.load(f)
                print("{} Clearing duplicate reply IDs.".format(screen_name))
                reply_ids = list(set(reply_ids))
                print("{} Saving backup reply IDs.".format(screen_name))
                with open('{}_reply_ids_backup.json'.format(screen_name), 'w') as f:
                    json.dump(reply_ids, f)
                print("{} Lenght of IDs list: {}".format(screen_name,len(reply_ids)))
            elif os.path.isfile('{}_reply_ids.json'.format(screen_name)) is False:
                print("Creating new ids file for {}.".format(screen_name))
                print("{} Length of new reply IDs list: {}.".format(screen_name,len(reply_ids)))

            #filter used ids from ids list
            print("{} Getting reply IDs.".format(screen_name))
            reply_ids = get_ids(screen_name,replytweets,reply_ids,len_counter_reply_ids)
            reply_ids = list(set(reply_ids))
            print("{} Savings reply IDs.".format(screen_name))
            with open('{}_reply_ids.json'.format(screen_name), 'w') as f:
                json.dump(reply_ids, f)
            print("{} Saving backup IDs.".format(screen_name))
            with open('{}_reply_ids_backup.json'.format(screen_name), 'w') as f:
                json.dump(reply_ids, f)
            print("{} Ajusted lenght of reply IDs list: {}".format(screen_name,len(reply_ids)))

            #GET REPLIES

            #Twitter API 3 tokens
            access_key = "1196733070210215937-kOtTpMiS1BFu7FYzEgFbkzmQKDyvGC"
            access_secret = "mLot2xban1C0774E2N9fJAzPJWkoRT31Iik4MHQtbZMcQ"
            consumer_key = "gMwo76vm414OWvYnkiNqyv6LE"
            consumer_secret = "3wMHuySlzbHrSvHXG6LFZ9M6vpHXJVVJTNx92QUTU2UHcN13hh"
            #authorize twitter, initialize tweepy
            api = Twython(consumer_key, consumer_secret, access_key, access_secret)

            counter = 0
            big_counter = 0
            reply_ids_chunks = [reply_ids[i:i+100] for i in range(0, len(reply_ids), 100)]
            for reply_id in reply_ids_chunks:
                try:
                    new_replytweets = api.lookup_status(id=reply_id,include_entities=True)
                    for reply in new_replytweets:
                        replytweets.append(reply)
                    print("{} {} raw comment tweets downloaded so far.".format(screen_name,len(replytweets)))
                    counter += 1
                    big_counter += 1
                    left = len_counter_reply_ids - len(replytweets)
                    if big_counter == 10:
                        print("{} counter reached {}, saving tweets.".format(screen_name,big_counter))
                        print("{} {} reply IDs left to finish".format(screen_name,left))
                        with open('{}_replytweets.json'.format(screen_name), 'w') as f:
                            json.dump(replytweets, f)
                        with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                            json.dump(replytweets, f)
                        print("{} {} REPLY TWEETS SAVED.".format(screen_name,len(replytweets)))
                    if counter == 1000:
                        print("{} counter reached {}, saving tweets.".format(screen_name,big_counter))
                        print("{} {} reply IDs left to finish".format(screen_name,left))
                        with open('{}_replytweets.json'.format(screen_name), 'w') as f:
                            json.dump(replytweets, f)
                        with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                            json.dump(replytweets, f)
                        print("{} {} REPLY TWEETS SAVED.".format(screen_name,len(replytweets)))
                        counter = 0
                except TwythonRateLimitError as error:
                    print("{} Error {} at reply id: {}".format(screen_name,error.error_code, replytweets[-1]['id']))
                    remainder = abs(float(api.get_lastfunction_header(header='x-rate-limit-reset')) - time.time())
                    del api
                    print("{} Resuming in {} seconds".format(screen_name,remainder))
                    time.sleep(remainder)
                    api = Twython(consumer_key, consumer_secret, access_key, access_secret)
                    with open('{}_replytweets.json'.format(screen_name), 'w') as f:
                        json.dump(replytweets, f)
                    with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                        json.dump(replytweets, f)
                    logging.basicConfig(level=logging.ERROR)
                    logging.error('This error occured: {}'.format(error))
                except TwythonError as error:
                    print("{} Error {} at reply id: {}".format(screen_name,error.error_code, replytweets[-1]['id']))
                    for i in reply_id:
                        reply_ids.remove(i)
                        for file in os.listdir(path):
                            if i == int(file):
                                os.remove(file)
                    with open('{}_reply_ids.json'.format(screen_name), 'w') as f:
                        json.dump(reply_ids, f)
                    with open('{}_reply_ids_backup.json'.format(screen_name), 'w') as f:
                        json.dump(reply_ids, f)
                    with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                        json.dump(replytweets, f)
                    pass

            #END OF REPLYTWEETS COLLECTION
            print("{} finished downloading {} replytweets. REPLY TWEETS COLLECTED. Saving files.".format(screen_name, len(replytweets)))
            with open('{}_replytweets.json'.format(screen_name), 'w') as f:
                json.dump(replytweets, f)
            with open('{}_replytweets_backup.json'.format(screen_name), 'w') as f:
                json.dump(replytweets, f)

        elif len(replytweets) == len_counter_reply_ids:
            print("{} already collected all {} replytweets.".format(screen_name,len(replytweets)))

    elif (os.path.isfile('{}_final_replytweets.json'.format(screen_name)) is True) and (os.path.isfile('{}_replytweets.json'.format(screen_name)) is True) :
        print("Resuming from old final_replytweets file for {}.".format(screen_name))
        with open('{}_final_replytweets.json'.format(screen_name), 'r') as f:
            final_replytweets = json.load(f)
        print("Resuming from old replytweets file for {}.".format(screen_name))
        with open('{}_replytweets.json'.format(screen_name), 'r') as f:
            replytweets = json.load(f)
        print("Resuming from old alltweets file for {}.".format(screen_name))
        with open('{}_alltweets.json'.format(screen_name), 'r') as f:
            alltweets = json.load(f)

        if len(final_replytweets) != len(replytweets):
            print("{} already collected all df_tweet.".format(screen_name))
            df_tweet = pd.read_csv('{}_df_tweet.csv'.format(screen_name))
            print("Resuming from old alltweets file for {}.".format(screen_name))
            with open('{}_alltweets.json'.format(screen_name), 'r') as f:
                alltweets = json.load(f)

            counter = 0
            big_counter = 0

            #filter replies
            print("{} filtering {} replytweets.".format(screen_name, len(replytweets)))
            final_replytweets = filter_replies(screen_name,alltweets,replytweets,final_replytweets,counter,big_counter)

            #clean output
    #                 print("{} Clearing duplicate final_replytweets.".format(screen_name))
    #                 final_replytweets = [i for n, i in enumerate(final_replytweets) if i not in final_replytweets[n + 1:]]
            print("{} {} directed comment tweets downloaded. SAVING.".format(screen_name,len(final_replytweets)))
            with open(screen_name+'_final_replytweets.json', 'w') as f:
                json.dump(final_replytweets, f)

        elif len(final_replytweets) == len(replytweets):
            print("{} finished collecting {} replies.".format(screen_name,len(alltweets)))

    #write DataFrame
    print("{} Writing df_reply.".format(screen_name))
    df_reply = write_df(screen_name,final_replytweets)
    print("{} Saving df_reply of length: {}.".format(screen_name,len(df_reply)))
    df_reply.to_json(screen_name+'_df_reply.json')
    df_reply.to_csv(screen_name+'_df_reply.csv',index=False)
    print("{} JSON and CSV files have been saved with {} replies. DONE!".format(screen_name,len(df_reply)))
    
    #Deleting TweetScraper replies files
    del_scraper(screen_name,password,main_dir,TweetScraper_path,saving_path_replies,files_path)
    
    return


# -

get_all("patriotact")

# +
# show_list = ["TheDailyShow","LastWeekTonight","SouthPark","nbcsnl","colbertlateshow","RealTimers","TheOnion",
#             "FullFrontalSamB","JimmyKimmelLive","LateNightSeth","zondagmetlubach","Lucky_TV","AC360","TuckerCarlson",
#             "hardball","CBSEveningNews","11thHour","NewsHour","ABCWorldNews","NightLine","FaceTheNation","60Minutes",
#             "NBCNews","MeetThePress","NOS","nosop3","RTLnieuws","Nieuwsuur","patriotact"]

# +
# list(map(get_all, show_list))

# +
# with concurrent.futures.ThreadPoolExecutor() as executor:
#     results = list(executor.map(get_all, show_list))

# +
# list(map(get_all, show_list))

# +
# screen_name = ""

# +
# get_all(screen_name)

# +
# if __name__ == '__main__':
#     while True:
#         engine9 = create_engine("mysql+pymysql://marksproject:IJKDEJFRknnkfr!!78278w2kjde@145.100.59.121/publicsphere?charset=utf8mb4")
#         con = engine9.connect()
#         user_to_get_comments = con.execute("SELECT User FROM StatusControl WHERE Platform = 'Twitter' AND Comments = 'Pending' LIMIT 1")
#         user_to_get_comments = user_to_get_comments.fetchall()
#         print(user_to_get_comments)
#         if len(user_to_get_comments) > 0:
#             screen_name = user_to_get_comments[0][0].replace('\ufeff', '')
#             print(screen_name)
#             con.execute("UPDATE StatusControl SET Comments = 'Ongoing' WHERE User ='"+screen_name+"' AND Platform = 'Twitter'")
#             print('Started with', screen_name)
# #             with open(screen_name+'_df_reply.json', 'r') as f:
# #                 data = json.load(f)
#             df_reply = pd.read_csv('{}_df_reply.csv'.format(screen_name))
#             df_reply.to_sql('Twitter_Comments', con=con, index=False, if_exists='append')
#             con.execute("UPDATE StatusControl SET Comments = 'Complete' WHERE User ='"+screen_name+"' AND Platform = 'Twitter'")
#         else:
#             break
