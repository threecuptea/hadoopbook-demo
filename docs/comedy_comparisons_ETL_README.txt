A. Introduction

- We have a set of data where viewers selected which of two YouTube Videos they prefer. We want to explore how we might use this data to make recommendations.

- You may use any tools to solve the exercises below. The focus is on showing your understanding of how to solve data problems.


B. Data Description

- We have 2 files in the comedy_comparisons.zip file, comedy_comparisons.train & comedy_comparisons.test. Each record of both files consists of 3 columns.

- The first column is the YouTube Video ID which was on the left side of the test & the second column is the video which was on the right. Videos can be viewed by replacing the respective Video ID in the URL below.

- The third column is the choice between the two videos made by the viewer, set to either left or right, based on which video the viewer liked more.
Sample data are as the followings:

sNabaB-eb3Y,y2emSXSE-N4,left
fY_FQMQpjok,sNabaB-eb3Y,left
Vr4D8xO2lBY,sNabaB-eb3Y,right
sNabaB-eb3Y,dDtRnstrefE,left


C. Exercises

1. Topic & Statistic extraction

- We would like to incorporate video data from YouTube's Freebase Index, & see if we can predict viewer Topic preferences.

- This step will require fetching Topics & Statistics for each Video from the YouTube Data API & re-organizing the training & test files. Use the following API calls below to fetch the Topic Data.

- https://www.googleapis.com/youtube/v3/videos?part=topicDetails,statistics&id=${VIDEO_ID}&key=AIzaSyDoJvvAj_1fiZWajF24I635VgJvdSHIQO0
- https://www.googleapis.com/youtube/v3/videos?part=topicDetails,statistics&id=wHkPb68dxEw&key=AIzaSyDoJvvAj_1fiZWajF24I635VgJvdSHIQO0
- Further information here : https://developers.google.com/youtube/v3/docs/videos

- Additional meta data on the Topics can be found in the Freebase API (You can skip this part since no report requirement on this)

https://www.googleapis.com/freebase/v1/topic${TOPIC_ID}
https://www.googleapis.com/freebase/v1/topic/m/0cnfvd

- Further information here : https://developers.google.com/freebase/v1/topic

2. Data Model

1. Flatten summary data into 2 fact tables/files with degenerate dimensions; no need to create dimension tables

- One table will be focused around Topics & the other around Videos

- Metrics include from statistics pull: viewCount, likeCount, dislikeCount, favoriteCount, commentCount

- Derived metrics from left/right data: leftPreferedCount, rightPreferedCount, leftRejectedCount, rightRejectedCount

- Topics Table will have Topic ID as the main dimension & Video Table will have Video ID as the main dimension

- Both Tables will have an indication if the data was from training or test file


D. Results

- Save all resulting files including results, code / workflows, topic & test intermediate files, etc.

- Document high level process of extraction, transformation & loading

- Interested in seeing a process that's robust & well thought out

