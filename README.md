### hadoopbook-demo collected an excellent full-fledge project comedy_comparison_etl as well other hadoop applications that I learned from the book Haddop-The Definitive Guide.  I passed Cloudera Certified Developer for Apache Hadoop (CCDH) by studying that book and practice it thoroughly.
#### The topics include:
   1. comedy_comparison_etl   
   
      This is an assigned exercise when I applied for Maker Studios.  I got a great review and later refined and 
      added JsonSerDe implementation.  
       
      To summarize, I have to create two matrices tables: __etl_video_matrix__ and __etl_topic_matrix__ (
      See _doc/comedy_comparisons_ETL_README.txt_ for the original requirement). 
        - Both metrics should include video statistics: viewCount, likeCount, dislikeCount, favoriteCount, 
          commentCount from Google YouTube Data API.  See _docs/sample-youtube-video-response.json_.
        - Both metrics should include derived statistics: leftPreferedCount, rightPreferedCount, leftRejectedCount
          , rightRejectedCount that come from processing data provided: comedy_comparisons.train, 
          comedy_comparisons.test in the format of                    
          _fY_FQMQpjok,sNabaB-eb3Y,left_   
          _Vr4D8xO2lBY,sNabaB-eb3Y,right_     
          left or right was set based on which video the viewer liked more
        - Both Tables will have an indication if the data was from training or test file
        
      To accomplish this (See _doc/comedy_comparisons_ETL-summary_ and _doc/notes-and-scripts_ for the details)
        - I wrote a Hadoop MapReduce job using MultipleOutputs: 
          * Output the video preference summary based upon comedy_comparison data to a Hive external table 
            etl_prefer_summary
          * Output jsons of individual video response from Google YouTube Data API to a Hive external table
            googleapis_youtube
        - Join etl_prefer_summary and googleapis_youtube to generate __etl_video_matrix__.
        - topicId is a nested array field of googleapis_youtube table.  Therefore, I created googleapis_youtube_exploded
          table by using LATERAL VIEW explode of googleapis_youtube table
        - Join googleapis_youtube_exploded table with etl_prefer_summary and group by topicid & source to generate 
          __etl_topic_matrix__.           
        - googleapis_youtube table was created using org.openx.data.jsonserde.JsonSerDe and with the schema composed of
          nested Hive STRUCT and ARRAY exactly matching YouTube video response so that I can query data naturally.     
        - It took time to retrieve video metadata/ statistics via Google YouTube Data API.  I use Secondary Sort 
          technique stated below to ensure I only call the API once for each video.
        - I combined VideoId and Source (SourceOrd 0 for train and 1 for test) into TextIntPair object as the key.   
          I created IdPartitioner (including VideoId only) as the PartitionerClass and GroupComparator (including 
          VideoId only) as the GroupingComparatorClass and KeyComparator (including both) as the SortComparatorClass
          so that all comedy_comparison records of the same video will go to the same Reducer and grouped under 
          the same key.  I will be able to call Google API only once for each video.
        - I use combiner to aggregate local data and reduce data shuffle between mapper and reducer.    
       
    