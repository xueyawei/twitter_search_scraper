'''
    Search Results's timezone is EST/DST UTC -04:00
    After Nov 6, 2016, timezone is EST UTC -5:00

    Author Yawei Xue 2016/11/02
'''

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib import parse
from datetime import datetime,timedelta,date
import mysql.connector
from selenium.common import exceptions


# init
# driver = webdriver.PhantomJS(executable_path=r"E:\Yawei Files\phantomjs-2.1.1-windows\bin\phantomjs")
driver = webdriver.Chrome()

# init mysql
config={
    'user':'root',
    'password':'admin',
    'host':'127.0.0.1',
    'database':'twitter_scrape'
}
add_tweet = ("INSERT INTO flight_jacket"
             "(id,timestamp_t,reply_count,retweet_count,like_count,location,text,hashtag,user_id,user_name,screen_name,permalink)"
             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
             )

#===========================
def url_generator(query,until):

    until = until
    query_in = query+" until:"+str(until.date())
    return "https://twitter.com/search?f=tweets&vertical=default&q="+parse.quote(query_in)+"&src=typd"

def get_location(tweet):
    location = ""
    try:
        location = tweet.find_element_by_class_name("stream-item-header")\
            .find_element_by_class_name("Tweet-geo").get_attribute("title")
    except Exception as e:
        try:
            location = tweet.find_element_by_class_name("stream-item-header") \
                .find_element_by_class_name("Tweet-geo").get_attribute("data-original-title")
        except Exception as e1:
            pass
    finally:
        return location

def get_hashtag(tweet):
    hashtag = ""
    hash_list = []
    try:
        hash_list = tweet.find_element_by_class_name("js-tweet-text-container").find_elements_by_tag_name("a")
        for tag in hash_list:
            if "twitter-hashtag" in tag.get_attribute("class").split(" "):
                hashtag+=tag.find_element_by_tag_name("b").text+","

    except Exception as e:
        pass
    finally:
        if len(hashtag)!=0:
            hashtag = hashtag[:-1]
        return hashtag



def sql_store(tweet,cnx,cursor):
    id = tweet.get_attribute("data-tweet-id")
    permalink = "https://twitter.com"+tweet.get_attribute("data-permalink-path")
    user_name = tweet.get_attribute("data-name")
    user_id = tweet.get_attribute("data-user-id")
    screen_name = tweet.get_attribute("data-screen-name")
    timestamp_t = tweet.find_element_by_class_name("tweet-timestamp").find_element_by_class_name("_timestamp") \
            .get_attribute("data-time")
    reply_count = tweet.find_element_by_class_name("stream-item-footer")\
        .find_element_by_class_name("ProfileTweet-actionCountList")\
        .find_element_by_class_name("ProfileTweet-action--reply")\
        .find_element_by_class_name("ProfileTweet-actionCount")\
        .get_attribute("data-tweet-stat-count")
    retweet_count = tweet.find_element_by_class_name("stream-item-footer")\
        .find_element_by_class_name("ProfileTweet-actionCountList")\
        .find_element_by_class_name("ProfileTweet-action--retweet")\
        .find_element_by_class_name("ProfileTweet-actionCount")\
        .get_attribute("data-tweet-stat-count")
    like_count = tweet.find_element_by_class_name("stream-item-footer")\
        .find_element_by_class_name("ProfileTweet-actionCountList")\
        .find_element_by_class_name("ProfileTweet-action--favorite")\
        .find_element_by_class_name("ProfileTweet-actionCount")\
        .get_attribute("data-tweet-stat-count")
    text = tweet.find_element_by_class_name("js-tweet-text-container")\
        .find_element_by_tag_name("p").text

    # may not have
    hashtag = get_hashtag(tweet)
    location = get_location(tweet)

    print("============= Execute SQL =============")
    try:
        cursor.execute(add_tweet, (id,timestamp_t,reply_count,retweet_count,like_count,location,text,hashtag,user_id,user_name,screen_name,permalink))
        cnx.commit()
    except:
        pass


def store_data(data,dt_obj):
    print("============= Store to MySQL =============")
    data_list = []
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    for tweet in data:
        timestamp = tweet.find_element_by_class_name("tweet-timestamp").find_element_by_class_name("_timestamp") \
            .get_attribute("data-time")
        date_obj = datetime.fromtimestamp((int)(timestamp))
        if date_obj>=dt_obj:
            sql_store(tweet,cnx,cursor)
            data_list.append(timestamp)
        else:
            break
    for dt in data_list:
        print(datetime.fromtimestamp((int)(dt)))
        print("-----------")
    cursor.close()
    cnx.close()



def load_page(q,until_date):
    until_date = until_date
    # Due to twitter timezone bug, until method cannot return data between 20:00-24:00(EST/DST UTC -4:00).
    # After 11-06-2016, delta should be timedelta(days=1,hours=5) due to DST is not used
    delta = timedelta(days=1,hours=4)
    driver.get(url_generator(q,until_date))
    content =driver.find_element_by_id("stream-items-id").find_elements_by_class_name("tweet")
    check_data(content,until_date-delta)


def check_data(data,since_date):
    is_store = False
    for tweet in data:
        timestamp = tweet.find_element_by_class_name("tweet-timestamp").find_element_by_class_name("_timestamp") \
            .get_attribute("data-time")
        timestamp = datetime.fromtimestamp((int)(timestamp))
        if timestamp< since_date:
            is_store = True
            break
    if is_store!=True:
        # get position
        position = driver.find_element_by_class_name("stream-container").get_attribute("data-min-position").split("-")[
            1]
        compare_pos = \
        driver.find_element_by_class_name("stream-container").get_attribute("data-min-position").split("-")[1]

        # scroll page
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # wait load
        while position == compare_pos:
            compare_pos = \
                driver.find_element_by_class_name("stream-container").get_attribute("data-min-position").split("-")[1]
        # loaded
        scroll_data = driver.find_element_by_id("stream-items-id").find_elements_by_class_name("tweet")
        check_data(scroll_data,since_date)
    else:
        store_data(driver.find_element_by_id("stream-items-id").find_elements_by_class_name("tweet"),since_date)


q = '"flight jacket"'
source_date = datetime(2016,10,5)
since_date = datetime(2016,9,30)


def main_loop(query,start,end):
    change_date = end
    while True:
        print("==================================")
        print(str((change_date-timedelta(days=1)))+" ===> "+str(change_date))
        load_page(query,change_date)
        if (change_date-timedelta(days=1))>start:
            change_date-=timedelta(days=1)
        else:
            break
    driver.quit()

main_loop(q,since_date,source_date)