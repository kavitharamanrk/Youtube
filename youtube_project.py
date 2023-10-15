
from googleapiclient.discovery import build

import pandas as pd

import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import streamlit as st
import pandas as pd
import numpy as np

import json
from bson.json_util import dumps, loads

import mysql.connector

import datetime
import re



api_key = "AIzaSyAlhBTp0He5Bv3ak5HsAzl8RDFagWj6FhQ"
yt = build('youtube', 'v3', developerKey=api_key)

dbname="Youtube_Channels"

def MongoDBConnection():
  try:
      client=MongoClient("mongodb+srv://kmongo:<password>@cluster0.xk9vmfy.mongodb.net/?retryWrites=true&w=majority")
      db = client.youtube
      Collection=db.channel
      print("Pinged your deployment. You successfully connected to MongoDB!")
  except Exception as e:
      print(e)
  return "MongoDB Connected"

def mySqlConnection(query,querytype):
  lis=[]
  try:
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="myData1",
    database=dbname,
    )
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)

    for x in mycursor:
        lis.append(x)
    mydb.close()  
  except Exception as e:
    print("SQL DB Connection error:",{e})
  finally:
    print("DB Connected")  
  print("DB Closed")
  return lis
    
# Channel
def get_channel_info(yt,channel_id):
  try:
    all_channel_data=[]
    all_channel_name=[]

    channel_type='General'
    request = yt.channels().list(
      part='id,snippet,contentDetails,statistics,status',
      id=channel_id)
    response = request.execute()
 
    for i in range(len(response['items'])):
        channel_name=response['items'][i]['snippet']['title'],
        channel_id=''.join(response['items'][i]['id']),
        channel_desc=response['items'][i]['snippet']['description'],
        subscribers= response['items'][i]['statistics']['subscriberCount'],
        channel_Views= response['items'][i]['statistics']['viewCount'],
        Videos = response['items'][i]['statistics']['videoCount'],
        channel_status = response['items'][i]['status']['privacyStatus']


        data=dict(_id=response['items'][i]['id'],
                  Channel_Name=response['items'][i]['snippet']['title'],
                  Subscription_Count= response['items'][i]['statistics']['subscriberCount'],
                  Channel_Views= response['items'][i]['statistics']['viewCount'],
                  Channel_Description=response['items'][i]['snippet']['description'],
                  Video_Count = response['items'][i]['statistics']['videoCount'],
                  Channel_Status = response['items'][i]['status']['privacyStatus'],
                  Playlist=get_pl_info(yt,response['items'][i]['id'])
                  )
        all_channel_data.append(data)
        all_channel_name.append(response['items'][i]['snippet']['title'])
  except Exception as e:
    print("channel data error:",{e})
  finally:
    print("channel executed")
  return all_channel_name,all_channel_data
 
# Play list
def get_pl_info(yt,channel_id):
  try:
    all_pl_data=[]
    prequest = yt.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
    )
    presponse = prequest.execute()
    for i in range(len(presponse['items'])):
      pl_id=presponse['items'][i]['id']
      pdata=dict(
                Playlist_Name= presponse['items'][i]['snippet']['title'],
                Playlist_Id=presponse['items'][i]['id'],
                Channel_id=channel_id,     
                Videos  =get_plitem_info(yt,pl_id)         
      )
      all_pl_data.append(pdata)
  
  except Exception as e:
    print("playlist data error:",{e})
  finally:
    print("playlist executed")
  return all_pl_data

# Play list item
def get_plitem_info(yt,pl_id):
  try:
    all_pli_data=[]
    pli_request = yt.playlistItems().list(
        part="snippet,contentDetails,status",
        playlistId=pl_id
    )
    pli_response = pli_request.execute()
    for i in range(len(pli_response['items'])):
        plidata=dict(  
                Videos  =get_video_info(yt,pli_response['items'][i]['contentDetails']['videoId'],pl_id)                      
        )
        all_pli_data.append(plidata)

  except Exception as e:
    print("playlist data error:",{e})
  finally:
    print("playlist executed")
  return all_pli_data

# Video data
def get_video_info(yt,video_id,pl_id):
  try:
    all_video_data=[]
    vrequest = yt.videos().list(
        part="snippet,contentDetails,statistics,status",maxResults=50,
        id=video_id
    )
    vresponse = vrequest.execute()
    comment_count,Commentsdata=get_comment_info(yt,video_id)
    
    for i in range(len(vresponse['items'])):
      key='tags'
      if key in vresponse['items'][i]['snippet'].keys():
        vtags= vresponse['items'][i]['snippet']['tags'],
      else:
        vtags='None'
      key='description'
      if key in vresponse['items'][i]['snippet'].keys():
        vdesc= vresponse['items'][i]['snippet']['description'],
      else:
        vdesc='None'
      key='dislikeCount'
      if key in vresponse['items'][i]['statistics'].keys():
        vdislikecount= vresponse['items'][i]['statistics']['dislikeCount'],
      else:
        vdislikecount=0   
      key='likeCount'
      if key in vresponse['items'][i]['statistics'].keys():
        vlikecount= vresponse['items'][i]['statistics']['likeCount'],
      else:
        vlikecount=0     
      totalduration=vresponse['items'][i]['contentDetails']['duration']
      vdata=dict(
                Video_Id=video_id,
                Channel_Id=vresponse['items'][i]['snippet']['channelId'],
                Playlist_Id=pl_id,
                Video_Name= vresponse['items'][i]['snippet']['title'],
                Video_Description= vdesc,
                Published_At= vresponse['items'][i]['snippet']['publishedAt'],       
                Tags= vtags,
                View_Count= vresponse['items'][i]['statistics']['viewCount'],
                Like_Count= vlikecount,
                Dislike_Count= vdislikecount,
                Favorite_Count= vresponse['items'][i]['statistics']['favoriteCount'],
                Comment_Count= comment_count ,                
                Duration= totalduration,
                Thumbnail= vresponse['items'][i]['snippet']['thumbnails']['default']['url'],
                Caption_Status= vresponse['items'][i]['status']['privacyStatus'],
                Comments=Commentsdata
      )
      all_video_data.append(vdata)    
  except Exception as e:
    Tags='None'
    pass
    print(f"An error occurred  {e}")
    Commentsdata=[]
  finally:
    print("Video Execution completed.")
  return all_video_data

# Comments
def get_comment_info(yt,video_id):
  try:
    all_comment_data=[]
    comment_count=0
    n=1
    crequest = yt.commentThreads().list(
        part="snippet", maxResults=50,
        videoId=video_id
    )
    cresponse = crequest.execute()

    for i in range(len(cresponse['items'])):
      # s="Comment_Id_"+ str(n)
      comment_count=comment_count+1
      # all_comment_data.append(s)
      cdata=dict(
          Comment_Id=cresponse['items'][i]['id'],
          Channel_Id= cresponse['items'][i]['snippet']['channelId'],
          Video_Id=cresponse['items'][i]['snippet']['videoId'],
          Comment_Author=cresponse['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
          Comment_Text=cresponse['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
          Comment_PublishedAt=cresponse['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
      )
      all_comment_data.append(cdata)
      n=n+1
  
  except Exception as e:
      print(f"An error occurred: {e}")
  finally:
      print("comments Execution completed.")
  return (comment_count,all_comment_data)


# Push to Mongo DB
def insert_to_mongodb(filename):
  try:
    # MongoDBConnection()
    client=MongoClient("mongodb+srv://kmongo:<password>@cluster0.xk9vmfy.mongodb.net/?retryWrites=true&w=majority")
    db = client.youtube
    Collection=db.channel
    with open(filename) as file:
        file_data = json.load(file)
        # print(type(file_data))
    if isinstance(file_data, list):
        Collection.insert_many(file_data)  
    else:
        Collection.insert_one(file_data)

  except Exception as e:
    print("Data insertion error:",{e})
    st.write(":red[Already Channel Pushed to Mongo DB]")
  finally:
    print("Processing....")
 
  return "Stored in Mongo DB"

# New JSON File Creation
def create_json(filename,json_file_data):
  json_data = dumps(json_file_data, indent = 2)
  with open(filename, 'w') as file:
    file.write(json_data)
  return "JSON created"

# Read JSON file
def read_json(filename):
    with open(filename, 'r') as jfileread:
        data=jfileread.read()
    obj = json.loads(data)
    obj

# Get youtube Channel id 
def get_channel_id(get_channelname):
  request = yt.search().list(q=get_channelname, type='channel', part='id')
  response = request.execute()
  channel_id = response['items'][0]['id']['channelId']
  request = yt.channels().list(part='statistics', id=channel_id)
  response = request.execute()
  return channel_id 

# Data load from Mongo DB
def Load_MongoDB():
  MongoDBConnection()
  yt_channel_data=Collection.find({Channel_name},{"_id":False})
  df=pd.DataFrame(yt_channel_data)
  
# SQL table creation
def create_sqltable():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="<password>",
    database=dbname,
    )
  mycursor = mydb.cursor(buffered=True)
  cquery="CREATE TABLE IF NOT EXISTS ytchannel(channel_id varchar(255) PRIMARY KEY NOT NULL,channel_name varchar(255),channel_type varchar(255),channel_views bigint,channel_description text,channel_status varchar(255),channel_videos int, channel_subscribers int)"
  mycursor.execute(cquery)
  pquery="CREATE TABLE IF NOT EXISTS ytplaylist(playlist_id varchar(255) PRIMARY KEY, channel_id varchar(255) , playlist_name varchar(255),FOREIGN KEY(channel_id) REFERENCES ytchannel(channel_id))"
  mycursor.execute(pquery)
  vquery="CREATE TABLE  IF NOT EXISTS ytvideo (video_id 	VARCHAR(255) PRIMARY KEY, playlist_id varchar(255), channel_id varchar(255), video_name VARCHAR(255),video_description TEXT,published_date VARCHAR(50), view_count INT, like_count bigint, dislike_count bigint, favorite_count INT, comment_count INT, duration VARCHAR(20), thumbnail VARCHAR(255),caption_status VARCHAR(255), tags TEXT,FOREIGN KEY(playlist_id) REFERENCES ytplaylist(playlist_id))"
  mycursor.execute(vquery)  
  cmmntquery="CREATE TABLE IF NOT EXISTS ytcomment (comment_id VARCHAR(255) , video_id varchar(255), channel_id varchar(255),comment_text TEXT, comment_author varchar(255),comment_published_date varchar(50),FOREIGN KEY(video_id) REFERENCES ytvideo(video_id))"
  mycursor.execute(cmmntquery)
  
# Duration Change
def Time_Change(strTime):
  if strTime.find("M")!=-1:
    strTime1=re.sub('M', " ",strTime,flags=re.IGNORECASE)
    strTime2=re.sub('S', "",strTime1,flags=re.IGNORECASE)
    strTime3=re.sub('PT', "00 ",strTime2,flags=re.IGNORECASE)
    strTime4=strTime3.split()
    s=int(strTime4[2])
    h=int(strTime4[0])*60*60
    m=int(strTime4[1])*60
  else:
    strTime2=re.sub('S', "",strTime,flags=re.IGNORECASE)
    strTime3=re.sub('PT', "00 ",strTime2,flags=re.IGNORECASE)
    strTime4=strTime3.split()
    s=int(strTime4[1])
    h=int(strTime4[0])*60*60
    m=0
  total=0
  total=h+m+s
  return total

# Mongo Db to SQL migration
def insert_channel(chname):
  try:
    MongoDBConnection()
    yt_channel_data=Collection.find({"Channel_Name":chname},{})
    # mySqlConnection()
    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="myData1",
      database=dbname,
      )
    mycursor = mydb.cursor(buffered=True)
    v_data=[]
    c_data=[]

# Channel insert
    for i in yt_channel_data:
      st.write(":blue[SQL Migration is in Progress.........]")
      sql = " INSERT INTO ytchannel (channel_id ,channel_name ,channel_type ,channel_views , channel_description ,channel_status ,channel_videos , channel_subscribers ) VALUES (%s, %s,%s, %s,%s, %s,%s, %s)"
      val = (i['_id'],''.join(i['Channel_Name']),"General",i["Channel_Views"],i["Channel_Description"],i["Channel_Status"],i["Video_Count"],i["Subscription_Count"])
      mycursor.execute(sql, val)
      mydb.commit()
  
# Playlist insert
      print("channel's playlist data is migrating to SQL.........")
      for j in range(len(i['Playlist'])):
        sql = " INSERT INTO  ytplaylist(playlist_id,channel_id , playlist_name) VALUES(%s,%s,%s)"
        val = (i['Playlist'][j]['Playlist_Id'],i['Playlist'][j]['Channel_id'],i['Playlist'][j]['Playlist_Name'])
        mycursor.execute(sql, val)
        mydb.commit()

        
# Videos insert
        print("channel's video data is migrating to SQL.........")
        v_data=i['Playlist'][j]['Videos']
        v_data.pop(0)
        try:
          for k in v_data:
            videoid=k['Videos'][0]['Video_Id']
            playlistid=k['Videos'][0]['Playlist_Id']
            channelid=k['Videos'][0]['Channel_Id']
            videoname=k['Videos'][0]['Video_Name']
            videodesc=''.join(k['Videos'][0]['Video_Description'])
            pubdt=k['Videos'][0]['Published_At']
            viewcnt=int(k['Videos'][0]['View_Count'])
            likecnt=int(''.join(k['Videos'][0]['Like_Count']))
            dislikecnt=int(k['Videos'][0]['Dislike_Count'])
            favcnt=int(k['Videos'][0]['Favorite_Count'])
            cmmntcnt=int(k['Videos'][0]['Comment_Count'])
            duration= int(Time_Change(k['Videos'][0]['Duration']))
            thumbnail=k['Videos'][0]['Thumbnail']
            captionstatus=k['Videos'][0]['Caption_Status']
            tags1=k['Videos'][0]['Tags']
            tags=''
            tags=''.join(tags1[0])
            print(videoid)
            c_data=k['Videos'][0]['Comments']
            sql = "INSERT IGNORE INTO ytvideo (video_id,playlist_id,channel_id,video_name, video_description ,published_date , view_count , like_count, dislike_count, favorite_count , comment_count , duration , thumbnail , caption_status,tags) VALUES (%s,%s, %s,%s, %s,%s,%s,%s, %s,%s, %s,%s, %s,%s,%s)"
            val = (videoid,playlistid,channelid,videoname,videodesc,pubdt,viewcnt,likecnt,dislikecnt,favcnt,cmmntcnt,duration,thumbnail,captionstatus,tags)
            mycursor.execute(sql, val)
            mydb.commit()

# Comments insert            
            for x in c_data:
              commentid=c_data[0]['Comment_Id']
              channelid=c_data[0]['Channel_Id']    
              videoid=c_data[0]['Video_Id']
              author=c_data[0]['Comment_Author']  
              cpubdt=c_data[0]['Comment_PublishedAt']
              cmnttxt=c_data[0]['Comment_Text']   
              sql = "INSERT INTO ytcomment (comment_id , video_id , channel_id,comment_text, comment_author ,comment_published_date )  VALUES (%s, %s,%s, %s,%s,%s)"
              val = (commentid,videoid,channelid,cmnttxt,author,cpubdt)
              mycursor.execute(sql, val)
              mydb.commit()
        except Exception as e:
          continue
  except IndexError as ie:
    print("Index error",{ie})
  except Exception as e:
    st.error("Channel already migrated to SQL")
  finally:
    st.success("Migration completed")
  return "Channel information collected"

# SQL Data for analysis
def show_data(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  pass
  return

# Bar chart for query2
def show_chart1(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  st.bar_chart(data, x='Channel Name', y='Video Count', color=None, width=0, height=0, use_container_width=True)
  pass
  return

# for query 4
def show_chart3(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  st.scatter_chart(data,x="Channel Name", y="Comment Count")
  pass
  return

# for query 7
def show_chart7(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  st.bar_chart(data, x='Channel Name', y='Video Count', color='#228B22', width=0, height=0, use_container_width=True)

  pass
  return

# for query 8
def show_chart8(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  st.line_chart(data, x='Channel Name', y='Average Duration(in Seconds)', color='#FF1493', width=0, height=0, use_container_width=True)
  pass
  return

# for query 9
def show_chart9(header,query,columnlist):
  st.subheader(header)
  lis1=mySqlConnection(query,'Select')
  data=pd.DataFrame(lis1,columns=columnlist)
  st.dataframe(data, hide_index=True)
  st.scatter_chart(data,x="Channel Name", y="Comments Count")
  pass
  return

st.set_page_config(
    page_title="Youtube Data Collection",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)  
st.subheader("Youtube Data Harvesting and Warehousing")
st.caption("Python, MongoDB, MySQL, Streamlit, Youtube") 

tab1, tab2, tab3,tab4,tab5 = st.tabs(["Data Store", "Mongo DB", "Migration", "Data Analysis","About"])
filename="Youtube_Channel.json"
with tab1:
    channel_id=st.text_input("Enter youtube channel id for collecting channel information:"," ")
    channel_data=dict
    if (st.button('Collect')):
        channelname,channel_data=get_channel_info(yt,channel_id)
        msg=create_json("Youtube_Channel.json",channel_data)
        data = pd.DataFrame(channelname, columns=['Channel Name'])
        st.dataframe(data, hide_index=True)
        st.write(":blue[Channel Data collected]")

with tab2:
    
    if (st.button('Push to MongoDB')):
        msg=insert_to_mongodb(filename)
        st.success(msg)
        read_json(filename)

with tab3:
    client=MongoClient("mongodb+srv://kmongo:myData1@cluster0.xk9vmfy.mongodb.net/?retryWrites=true&w=majority")
    db = client.youtube
    Collection=db.channel
    yt_channel_data=Collection.find({},{"_id":False})
    ch_list=[]
    for i in yt_channel_data:
        ch_list.append(''.join(i['Channel_Name']))    
    chname=st.selectbox("Select Channel Name for SQL Migration: ",ch_list)
        
    if(st.button("Migrate to SQL")):
        # create_sqltable()
        print(chname)
        insert_channel(chname)

with tab4:
      
    query_lst=['1.What are the names of all the videos and their corresponding channels?',
            '2.Which channels have the most number of videos, and how many video s do they have?',
            '3.What are the top 10 most viewed videos and their respective channels?',
            '4.How many comments were made on each video, and what are their corresponding video names?',
            '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7.What is the total number of views for each channel, and what are their corresponding channel names? ',
            '8.What are the names of all the channels that have published videos in the year 2022?',
            '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
            ]
    qryselect=st.selectbox("Select your queries",query_lst)
    if qryselect==query_lst[0]:
        header="Channels and Videos"
        query="SELECT ytchannel.channel_name,ytvideo.video_name FROM ytchannel INNER JOIN ytvideo ON ytchannel.channel_id=ytvideo.channel_id"
        columnlist=['Channel Name','Video Name']
        show_data(header,query,columnlist)
 
    elif qryselect==query_lst[1]:
        header="Highest no.of videos"
        query="select channel_name, channel_videos from ytchannel order by channel_videos desc"
        columnlist=['Channel Name','Video Count']
        show_chart2(header,query,columnlist)   
  
    elif qryselect==query_lst[2]:
        header="Top 10 videos mostly viewed"
        query=" SELECT  ytchannel.channel_name,  ytvideo.video_name, ytvideo.view_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id ORDER BY ytvideo.view_count desc LIMIT 10"
        columnlist=['Channel Name','Video Name','View Count']
        show_data(header,query,columnlist)  
    elif qryselect==query_lst[3]:
        header="Highest comments"
        query="SELECT  ytchannel.channel_name,  ytvideo.video_name, ytvideo.comment_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id ORDER by ytchannel.channel_name, ytvideo.comment_count desc"
        columnlist=['Channel Name','Video Name','Comment Count']
        show_chart3(header,query,columnlist)  
    elif qryselect==query_lst[4]:
        header="Higest Likes"
        query="SELECT ytchannel.channel_name,  ytvideo.video_name, ytvideo.like_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id ORDER by ytchannel.channel_name, ytvideo.like_count desc"
        columnlist=['Channel Name','Video Name','Like Count']
        show_data(header,query,columnlist)  
    elif qryselect==query_lst[5]:
        header="Videos Like and Dislike "
        query="SELECT ytchannel.channel_name,  ytvideo.video_name, ytvideo.like_count,ytvideo.dislike_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id  ORDER by ytchannel.channel_name desc"
        columnlist=['Channel Name','Video Name','Likes','Dislikes']
        show_data(header,query,columnlist)  
    elif qryselect==query_lst[6]:
        header="Total views"
        query=" SELECT  ytchannel.channel_name,  ytvideo.video_name, ytvideo.view_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id "
        columnlist=['Channel Name','Video Name','Total Views']
        show_data(header,query,columnlist)  
    elif qryselect==query_lst[7]:
        header="Videos published on 2022"
        query="SELECT ytchannel.channel_name, count(ytvideo.video_id) FROM ytchannel INNER JOIN ytvideo ON ytchannel.channel_id=ytvideo.channel_id WHERE published_date LIKE ('%2022%') group by channel_name"
        columnlist=['Channel Name','Video Count']
        show_chart7(header,query,columnlist)      
    elif qryselect==query_lst[8]:
        header="Average duration of videos"
        query="SELECT ytchannel.channel_name,ROUND(AVG(ytvideo.duration))   FROM ytvideo INNER JOIN ytchannel ON ytvideo.channel_id=ytchannel.channel_id GROUP BY ytchannel.channel_name"
        columnlist=['Channel Name','Average Duration(in Seconds)']
        show_chart8(header,query,columnlist)  
    elif qryselect==query_lst[9]:
        header="Highes No. of Comments"
        query=" SELECT  ytchannel.channel_name,  ytvideo.video_name, ytvideo.comment_count from ytvideo INNER JOIN ytchannel ON ytchannel.channel_id=ytvideo.channel_id ORDER BY ytvideo.comment_count desc"
        columnlist=['Channel Name','Video Name','Comments Count']
        show_chart9(header,query,columnlist)    

with tab5:
  st.markdown("* :blue[This Streamlit application allows users to access and analyze data from multiple YouTube channels.]")
  st.markdown("* :blue[In Data Store tab input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.]")
  st.markdown("* :blue[In Mongo DB tab Option to store the data in a MongoDB database as a data lake.]")
  st.markdown("* :blue[In Migration tab there is a Option to select a channel name and migrate its data from the data lake to a SQL database as tables.]")
  st.markdown("* :blue[In Data Analysis tab select the given query and retrieve data for getting Youtube video channel related details.]")


    