import os
import wget
import time
import string
from psaw import PushshiftAPI
import logging
import datetime as dt
import pandas as pd
import fasttext
import sqlite3
from sqlite3 import Error
import praw
import reddit_dictionaries
from credentials import user_agent, client_id, client_secret, username, password

logging.basicConfig(level=logging.DEBUG)
table = str.maketrans(dict.fromkeys(string.punctuation))
filterlist = ["de", "en", "pt"]

reddit = praw.Reddit(
    user_agent=user_agent,
    client_id=client_id,
    client_secret=client_secret,
    username=username,
    password=password
)

api = PushshiftAPI(reddit)

data_dict = reddit_dictionaries.data_dict

q = "COVD19|Coronavirus|COVID-19|2019nCoV|Outbreak|coronavirus|WuhanVirus|covid19|2019ncov|corona|chinavirus|Pandemia|Epidemia|Ausbrauch|Pandemie"

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
timenow = time.strftime("%d%m%Y-%H%M%S")
logfile = f"rdt-{timenow}.log"

### Method for creating and entering log entries
def LOG_insert(file, format, text, level):
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(format)
    logger = logging.getLogger(file)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)
    infoLog.close()
    logger.removeHandler(infoLog)
    return

if os.path.isfile("lid.176.bin") == False:
    LOG_insert(logfile, formatLOG, f"Downloading language models from FastText", logging.INFO)
    modelurl ="https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
    wget.download(modelurl)

pretrained_model_langdect = "lid.176.bin"
model_langdect = fasttext.load_model(pretrained_model_langdect)

### Creates SQL database for processed data
def create_connection(db_file = "database.db", collect_file = "collect_data.db"):
    conn = None
    try:
        LOG_insert(logfile, formatLOG, f"Creating databases...", logging.INFO)
        conn = sqlite3.connect(db_file)
        conn.close()
    except Error as e:
        LOG_insert(logfile, formatLOG, f"Error creating database: {e}", logging.ERROR)
        quit()
    finally:
        if conn:
            conn.close()

def log_submission_comments(api_call, d_dict):
   batchsize = 100
   cmt_buffer = list()
   for submission in api_call:
      for key in d_dict['submissions'].keys():
         if hasattr(submission, key):
            d_dict['submissions'][key].append(getattr(submission, key))
         else:
            d_dict['submissions'][key].append("N/A")

      submission.comments.replace_more(limit=None)
      for item in submission.comments.list():
         cmt_buffer.append(item.id)
   cmt_buffer = ["t1_" + s for s in cmt_buffer]

   comment_gen = reddit.info(cmt_buffer)
   for comment in comment_gen:     
      for key in d_dict['comments'].keys():    
         if hasattr(comment, key):
            d_dict['comments'][key].append(getattr(comment, key))
         else:
            d_dict['comments'][key].append("N/A")

   user_buffer = d_dict['submissions']['author_fullname'] + d_dict['comments']['author_fullname']
   user_buffer = list(set(user_buffer))

   user_gen = reddit.redditors.partial_redditors(user_buffer)
   for user in user_gen:
      for key in d_dict['users'].keys():
         if hasattr(user, key):
            d_dict['users'][key].append(getattr(user, key))
         else:
            d_dict['users'][key].append("N/A")

   sub_buffer = d_dict['submissions']['subreddit_id']
   sub_buffer = list(set(sub_buffer))
   subreddit_gen = reddit.info(sub_buffer)
   for subreddit in subreddit_gen:
      for key in d_dict['subreddits'].keys():
         if hasattr(subreddit, key):
            d_dict['subreddits'][key].append(getattr(subreddit, key))
         else:
            d_dict['subreddits'][key].append("N/A")


def apply_language_detection(testdata):
    title = testdata['title'].astype(str).tolist()
    lang_title_t = [model_langdect.predict(item) for item in title]
    lang = [title[0] for title in lang_title_t]
    conf = [title[1] for title in lang_title_t]
    for i in range(len(conf)):
        if conf[i] >= 0.8:
            testdata.loc[i, 'lang'] = lang[i]

            testdata.loc[i, 'lang_conf'] = conf[i]
        else:
            testdata.loc[i, 'lang'] = "None"
            testdata.loc[i, 'lang_conf'] = "<0.80"

    body = testdata['selftext'].astype(str).tolist()
    body = [s.replace("\n", "") for s in body]
    lang_body_t = [model_langdect.predict(item) for item in body]
    lang2 = [title[0] for title in lang_body_t]
    conf2 = [title[1] for title in lang_body_t]
    for i in range(len(conf)):
        if conf[i] >= 0.8:
            continue
        elif conf[i] < 0.8 and conf2[i] >= 0.8:
            testdata.loc[i, 'lang'] = lang2[i]
            testdata.loc[i, 'lang_conf'] = conf2[i]
        else:
            testdata.loc[i, 'lang'] = "None"
            testdata.loc[i, 'lang_conf'] = "<0.80"
    testdata['lang'] = testdata['lang'].map(lambda x: x.replace("__label__", ""))


def cleanup_threads(dirty_sub_db):
    try:
        global table, filterlist
        cleandb = dirty_sub_db.query("lang in @filterlist")
        cleandb.loc[:,"proc_title"] = cleandb.loc[:,'title'].apply(lambda x : str.lower(x))
        cleandb.loc[:,"proc_selftext"] = cleandb.loc[:,'selftext'].apply(lambda x : str.lower(x))

        cleandb.loc[:,"proc_title"] = cleandb.loc[:,'proc_title'].apply(lambda x : x.translate(table))
        cleandb.loc[:,"proc_selftext"] = cleandb.loc[:,'proc_selftext'].apply(lambda x : x.translate(table))
    except Exception as e:
        LOG_insert(logfile, formatLOG , f"Error cleaning submission data: {e}", logging.ERROR)
        quit()
    return cleandb


def cleanup_comments(subdb, dirty_cmt_db):
    try:
        global table
        delvector = subdb["id"].astype(str).tolist()
        delvector = ["t3_" + s for s in delvector]
        cleandb = dirty_cmt_db.query("link_id in @delvector")
        cleandb.loc[:,"proc_body"] = cleandb.loc[:,'body'].apply(lambda x : str.lower(x))
        cleandb.loc[:,"proc_body"] = cleandb.loc[:,'proc_body'].apply(lambda x :x.translate(table))
        cleandb.loc[:,"proc_body"] = cleandb.loc[:,"proc_body"].replace(r'\n',' ', regex=True) 
    except Exception as e:
        LOG_insert(logfile, formatLOG , f"Error cleaning comment data: {e}", logging.ERROR)
        quit()
    return cleandb

def collect_subms_comments_users(batchsize = 50, start_point = "22/03/2020"):
    create_connection()
    LOG_insert(logfile, formatLOG, f"Initiating data collection from {start_point} with a batchsize of {batchsize}...", logging.INFO)
    endtime = dt.datetime.strptime(start_point, "%d/%m/%Y")
    rundate = dt.datetime.today()
    while endtime <= rundate: ### DAY LOOP LEVEL
        day_str = rundate.date()
        after = int((rundate - dt.timedelta(days=1)).timestamp())
        before = int((rundate).timestamp())
        batch = 1
        LOG_insert(logfile, formatLOG, f"Collecting batch {batch} from {day_str} - Unix Timecodes: b = {before}, a = {after}...", logging.INFO)
        api_request_generator = api.search_submissions(q = q, after = after, before= before, limit = batchsize)
        log_submission_comments(api_request_generator, data_dict)
        b_threads = pd.DataFrame.from_dict(data_dict['submissions'])
        b_comments = pd.DataFrame.from_dict(data_dict['comments'])
        b_subreddits = pd.DataFrame.from_dict(data_dict['subreddits'])
        b_users = pd.DataFrame.from_dict(data_dict['users'])
        prerowcount = len(b_threads.index)
        LOG_insert(logfile, formatLOG, f"Applying language detection in both title and text to batch {batch} from {day_str}", logging.INFO)
        apply_language_detection(b_threads)
        clean_b_threads = cleanup_threads(b_threads)
        clean_b_comments = cleanup_comments(clean_b_threads, b_comments)
        reddit_dictionaries.create_network(clean_b_threads, clean_b_comments, data_dict)
        b_network = pd.DataFrame.from_dict(data_dict['network'])
        postrowcount = len(clean_b_threads.index)
        cmt_count = len(clean_b_comments.index)
        rd_collect = pd.DataFrame({"day": [day_str],  "batch": [batch], "dataset_posts": [prerowcount], "cleaned_posts": [postrowcount], "comments": [cmt_count]})
        LOG_insert(logfile, formatLOG, f"Sending day {day_str}, batch {batch} data to database...", logging.INFO)
        conn = sqlite3.connect("database.db")
        clean_b_threads.to_sql("rd_threads", con = conn, if_exists= "append", index= False)
        clean_b_comments.to_sql("rd_comments", con = conn, if_exists= "append", index= False)
        b_subreddits.to_sql("rd_subreddits", con = conn, if_exists= "append", index= False)
        b_users.to_sql("rd_users", con = conn, if_exists= "append", index= False)
        b_network.to_sql("rd_network", con = conn, if_exists= "append", index= False)
        LOG_insert(logfile, formatLOG, f"Sending day {day_str}, batch {batch} numbers to collection database...", logging.INFO)
        rd_collect.to_sql("rd_collect", con = conn, if_exists= "append", index= False)
        with open("sanity.sql", 'r') as sql_file:
            conn.executescript(sql_file.read())    
        conn.close()
        reddit_dictionaries.empty_batch_storage(data_dict)
        if len(b_threads.index) == batchsize:
            while len(b_threads.index) == batchsize:
                print("Waiting for 5 seconds to be API polite...")
                time.sleep(5)
                batch += 1
                new_before = int(b_threads['created_utc'].iloc[-1])
                LOG_insert(logfile, formatLOG, f"Collecting batch {batch} from {day_str} Unix Timecodes: b = {new_before}, a = {after}...", logging.INFO)
                api_request_generator = api.search_submissions(q = q, after = after, before= new_before, limit = batchsize)
                log_submission_comments(api_request_generator, data_dict)
                b_threads = pd.DataFrame.from_dict(data_dict['submissions'])
                b_comments = pd.DataFrame.from_dict(data_dict['comments'])
                b_subreddits = pd.DataFrame.from_dict(data_dict['subreddits'])
                b_users = pd.DataFrame.from_dict(data_dict['users'])
                prerowcount = len(b_threads.index)
                LOG_insert(logfile, formatLOG, f"Applying language detection in both title and text to batch {batch} from {day_str}", logging.INFO)
                apply_language_detection(b_threads)
                clean_b_threads = cleanup_threads(b_threads)
                clean_b_comments = cleanup_comments(clean_b_threads, b_comments)
                reddit_dictionaries.create_network(clean_b_threads, clean_b_comments, data_dict)
                b_network = pd.DataFrame.from_dict(data_dict['network'])
                postrowcount = len(clean_b_threads.index)
                cmt_count = len(clean_b_comments.index)
                rd_collect = pd.DataFrame({"day": [day_str],  "batch": [batch], "dataset_posts": [prerowcount], "cleaned_posts": [postrowcount], "comments": [cmt_count]})
                LOG_insert(logfile, formatLOG, f"Sending day {day_str}, batch {batch} data to database...", logging.INFO)
                conn = sqlite3.connect("database.db")
                clean_b_threads.to_sql("rd_threads", con = conn, if_exists= "append", index= False)
                clean_b_comments.to_sql("rd_comments", con = conn, if_exists= "append", index= False)
                b_subreddits.to_sql("rd_subreddits", con = conn, if_exists= "append", index= False)
                b_users.to_sql("rd_users", con = conn, if_exists= "append", index= False)
                b_network.to_sql("rd_network", con = conn, if_exists= "append", index= False)
                LOG_insert(logfile, formatLOG, f"Sending day {day_str}, batch {batch} numbers to collection database...", logging.INFO)
                rd_collect.to_sql("rd_collect", con = conn, if_exists= "append", index= False)
                with open("sanity.sql", 'r') as sql_file:
                    conn.executescript(sql_file.read())    
                conn.close()
                reddit_dictionaries.empty_batch_storage(data_dict)         
                if len(b_threads.index) != batchsize:
                    rundate = rundate - dt.timedelta(days=1)
                    print("Waiting for 60 seconds for next day to be API polite...")
                    time.sleep(60)
                    break
        else:
            rundate = rundate - dt.timedelta(days=1)
            print("Waiting for 60 seconds for next day to be API polite...")
            time.sleep(60)
    return

collect_subms_comments_users(start_point="04/09/2021")