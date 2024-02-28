from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import streamlit as st


def authentication():
    api_key='AIzaSyCgZp7Z5Wnwz7xcKTEplCU0IAYYJhA4yqY'
    youtube = build('youtube', 'v3',developerKey=api_key)
    return youtube
 
youtube=authentication()


def get_channel_info(channel_id):
    request = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails'
    )
    response = request.execute()

    for i in response["items"]:
        data = dict(channel_name=i['snippet']['title'],
                    channel_id=i['id'],
                    subscription=i['statistics']['subscriberCount'],
                    channel_views=i['statistics']['viewCount'],
                    channel_desc=i['snippet']['description'],
                    playlist_ids=i['contentDetails']['relatedPlaylists']['uploads'])
    return data    

def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(part='snippet', playlistId=playlist_Id,
                                                 maxResults=50,
                                                 pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def get_video_info(video_ids):
    video_ids=video_ids[:2]
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part='snippet,contentDetails,statistics', id=video_id)
        response = request.execute()

        for item in response['items']:
            data = dict(channel_Name=item['snippet']['channelTitle'],
                        channel_Id=item['snippet']['channelId'],
                        video_Id=item['id'],
                        title=item['snippet']['title'],
                        tags=item['snippet'].get('tags'),
                        thumbnail=item['snippet']['thumbnails']['default']['url'],
                        description=item['snippet'].get('description'),
                        publishedDate=item['snippet']['publishedAt'],
                        duration=item['contentDetails']['duration'],
                        views=item['statistics'].get('viewCount'),
                        likes=item['statistics'].get('likeCount'),
                        comments=item['statistics'].get('commentCount'),
                        favCount=item['statistics']['favoriteCount'],
                        definition=item['contentDetails']['definition'],
                        captionstatus=item['contentDetails']['caption'])
            video_data.append(data)
            sorted_video_data = sorted(video_data, key=lambda x: x['comments'], reverse=True)
            return sorted_video_data

def get_comments_info(video_ids):
    video_ids=video_ids[:2]
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part='snippet', videoId=video_id, maxResults=2)
            response = request.execute()

            for item in response['items']:
                data = dict(comment_id=item['snippet']['topLevelComment']['id'],
                            video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data   


client = MongoClient("mongodb+srv://saraswathigolewar:golewar@cluster0.y3rf2ss.mongodb.net/")
db=client["YouTube_Data"]

#Uploading to MongoDB
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vd_ids = get_videos_ids(channel_id)
    vd_info = get_video_info(vd_ids)
    com_details = get_comments_info(vd_ids)

    col = db['channel_details']
    col.insert_one({"channel_info": ch_details, "video_info": vd_info, "comment_info": com_details})

    return "upload completed successfully"

#Table creation for channels,Videos, comments
def channels_table():
    connection = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube')
    mycursor = connection.cursor()


    try:
        query = '''create table if not exists channels
                    (
                        channel_name varchar(100),
                        channel_id varchar(80) primary key,
                        subscription bigint,
                        channel_views bigint,
                        channel_desc text,
                        playlist_ids varchar(80)
                    )'''
        mycursor.execute(query)
        connection.commit()
    except:
        st.write("channel table already created")  

    ch_list = []
    db = client["YouTube_Data"]
    col = db['channel_details']
    for ch_data in col.find({}, {"_id": 0, "channel_info": 1}):
        ch_list.append(ch_data["channel_info"])
    df = pd.DataFrame(ch_list) 

    for index, row in df.iterrows():
        query = '''INSERT INTO channels(channel_name,
                                        channel_id,
                                        subscription,
                                        channel_views,
                                        channel_desc,
                                        playlist_ids)
                                        
                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row['channel_name'],
                  row['channel_id'],
                  row['subscription'],
                  row['channel_views'],
                  row['channel_desc'],
                  row['playlist_ids'])
    try:
        mycursor.execute(query, values)
        connection.commit()
    except:
        st.write('channel values are already inserted')


def videos_table():
    connection = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube')
    mycursor = connection.cursor()

    try:
        query = '''create table if not exists videos
                ( channel_Name varchar(100),
                channel_Id varchar(100),
                video_Id varchar(100) primary key,
                title varchar(255),
                tags text,
                thumbnail varchar(255),
                description text,
                publishedDate varchar(30),
                duration varchar(50),
                views bigint,
                likes bigint,
                comments int,
                favCount int,
                definition varchar(50),
                captionstatus varchar(50)
                )'''
        mycursor.execute(query)
        connection.commit()

    except:
        st.write("Videos Table alrady created")

    vi_list = []
    db = client["YouTube_Data"]
    col = db['channel_details']
    for vi_data in col.find({}, {"_id": 0, "video_info": 1}):
        for i in range(len(vi_data["video_info"])):
            vi_list.append(vi_data["video_info"][i])

    df2 = pd.DataFrame(vi_list) 

    for index, row in df2.iterrows():
        tags_str = ','.join(row['tags']) if isinstance(row['tags'], list) else None
        query_check = '''SELECT video_Id FROM videos WHERE video_Id = %s'''
        mycursor.execute(query_check, (row['video_Id'],))
        existing_record = mycursor.fetchone()
        if existing_record:
    # Record already exists, skip insertion
           continue
        else:
            query = '''insert into videos(channel_Name,
                                            channel_Id,
                                            video_Id,
                                            title,
                                            tags,
                                            thumbnail,
                                            description,
                                            publishedDate,
                                            duration,
                                            views,
                                            likes,
                                            comments,
                                            favCount,
                                            definition,
                                            captionstatus
                                        )

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values = (row['channel_Name'],
                    row['channel_Id'],
                    row['video_Id'],
                    row['title'],
                    tags_str,
                    row['thumbnail'],
                    row['description'],
                    row['publishedDate'],
                    row['duration'],
                    row['views'],
                    row['likes'],
                    row['comments'],
                    row['favCount'],
                    row['definition'],
                    row['captionstatus']
                    )
            try:
                mycursor.execute(query, values)
                connection.commit()
            except mysql.connector.Error as err:
                if err.errno == mysql.connector.errorcode.ER_LOCK_WAIT_TIMEOUT:
                    st.write("Error: Timeout waiting for table metadata lock. Retrying...")
                    # Retry the transaction or implement backoff strategy
                else:
                    st.write("Error:", err)


def comments_table():
    connection = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube')
    mycursor = connection.cursor()


    try:
        query = '''create table if not exists comments
                    ( comment_id varchar(100) primary key,
                    video_Id varchar(100),
                    comment_text text,
                    comment_author varchar(100),
                    comment_published varchar(100)
                    )'''
        mycursor.execute(query)
        connection.commit()
    except:
        st.write("Commentsp Table already created")    

    cm_list = []
    db = client["YouTube_Data"]
    col = db['channel_details']
    for cm_data in col.find({}, {"_id": 0, "comment_info": 1}):
        for i in range(len(cm_data["comment_info"])):
            cm_list.append(cm_data["comment_info"][i])

    df3 = pd.DataFrame(cm_list) 

    for index, row in df3.iterrows():
            query = '''INSERT INTO comments(comment_id,
                                            video_Id ,
                                            comment_text ,
                                            comment_author ,
                                            comment_published
                                        )

                                        values(%s,%s,%s,%s,%s)'''

            values = (
                row['comment_id'],
                row['video_Id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published'],
                
            )
    try:    
        mycursor.execute(query, values)
        connection.commit()
    except:
        st.write("This comments are already exist in comments table")    


def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"


def show_channels_table():
    ch_list=[]
    db=client["YouTube_Data"]
    col1 = db['channel_details']
    for ch_data in col1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])
    channels_table=st.dataframe(ch_list)
    return channels_table

     
def show_videos_table():
    vi_list=[]
    db=client["YouTube_Data"]
    col2 = db['channel_details']
    for vi_data in col2.find({},{"_id":0,"video_info":1}):
        for i in range(len(vi_data["video_info"])):
         vi_list.append(vi_data["video_info"][i])
    videos_table=st.dataframe(vi_list)  
    return videos_table 



def show_comments_table():
  cm_list=[]
  db=client["YouTube_Data"]
  col3 = db['channel_details']
  for cm_data in col3.find({},{"_id":0,"comment_info":1}):
      for i in range(len(cm_data["comment_info"])):
        cm_list.append(cm_data["comment_info"][i])
  comments_table=st.dataframe(cm_list)  
  return comments_table    




#streamlit

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills Take Away:")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channel_ids = st.text_input("Enter the channel IDs separated by commas")

# Split the input into a list of channel IDs
channel_ids_list = [channel_id.strip() for channel_id in channel_ids.split(",")]

if st.button("Collect and store data"):
    # Iterate over each channel ID in the list and process it
    for channel_id in channel_ids_list:
        ch_ids = []
        db = client["YouTube_Data"]
        col = db['channel_details']
        for ch_data in col.find({}, {"_id": 0, "channel_info": 1}):
            ch_ids.append(ch_data["channel_info"]["channel_id"])

        if channel_id in ch_ids:
            st.success(f"Channel details for {channel_id} already exist")
        else:
            insert = channel_details(channel_id)
            st.success(insert)

if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)
 
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()
elif show_table=="VIDEOS":
    show_videos_table()
elif show_table=="COMMENTS":
    show_comments_table() 


connection = mysql.connector.connect(host='localhost',user='root',password='12345',database='youtube')
mycursor = connection.cursor()
questions = st.selectbox('Questions',['1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

if questions == '1. What are the names of all the videos and their corresponding channels?':
    query = """SELECT title AS Video_name, channel_Name AS Channel_Name
                        FROM videos
                        ORDER BY channel_Name"""
    mycursor.execute(query)
    results = mycursor.fetchall()
    df = pd.DataFrame(results, columns=mycursor.column_names)
    st.write(df)

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    mycursor.execute("""SELECT channel_Name AS Channel_Name, COUNT(*) AS Total_Videos
                        FROM videos
                        GROUP BY channel_Name
                        ORDER BY Total_Videos DESC""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Number of videos in each channel :]",df)

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    mycursor.execute("""SELECT channel_Name AS Channel_Name, title AS Video_Title, views AS Views 
                        FROM videos
                        ORDER BY views DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Top 10 most viewed videos :]",df)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    mycursor.execute("""SELECT a.video_Id AS Video_id, title AS Video_Title, b.Total_Comments
                        FROM videos AS a
                        LEFT JOIN (SELECT video_Id, COUNT(comment_id) AS Total_Comments
                        FROM comments GROUP BY video_Id) AS b
                        ON a.video_Id = b.video_Id
                        ORDER BY b.Total_Comments DESC""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_Name AS Channel_Name, title AS Video_Title, likes AS Like_Count 
                        FROM videos
                        ORDER BY likes DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Top 10 most liked videos :]",df)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    mycursor.execute("""SELECT title AS Title, likes AS Like_count
                        FROM videos
                        ORDER BY Like_count DESC""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_Name AS Channel_Name, SUM(views) AS Total_Views
                        FROM videos
                        GROUP BY channel_Name
                        ORDER BY Total_Views DESC""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Total views per channel :]",df)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    mycursor.execute("""SELECT DISTINCT channel_Name AS Channel_Name
                        FROM videos
                        WHERE publishedDate LIKE '2022%'""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_Name AS Channel_Name,
                        AVG(duration) AS Avg_Duration
                        FROM videos
                        GROUP BY channel_Name
                        ORDER BY Avg_Duration DESC""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Average video duration for each channel :]",df)

elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    mycursor.execute("""select title as VideoTitle, channel_Name as ChannelName, comments as Comments from videos 
                       where Comments is not null order by Comments DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write("### :green[Top 10 videos with the most comments :]",df)
