# YouTube data harvesting and warehousing
YouTube data harvesting and warehousing using MYSQL, Mongodb and Streamlit. This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a MYSQL data warehouse, and enables users to view the data in the Streamlit app.

# Problem Statement
The task is to build a Streamlit app that permits users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data like channel information, video details, and user engagement. The app should facilitate storing the data in a MongoDB database and allow users to collect data from up to 10 different channels. Additionally, it should offer the capability to migrate selected channel data from the data lake to a MYSQL database for further analysis. The app should enable searching and retrieval of data from the MYSQL database, including advanced options like joining tables for comprehensive channel information.

# Technology Stack Used

1.Python

2.MySQL

3.MongoDB

4.Google Client Library

# Approach

1.Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.

2.Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 3 different collections.

3.Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL or PostgreSQL for this purpose.

4.Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the the foreign and the primary key.

5.The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data.
