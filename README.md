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
            etl_prefer_summary by using the combinaton of
              _MultipleOutputs.addNamedOutput(job, "prefer", TextOutputFormat.class, Text.class, Text.class);_
              _multipleOutputs.write("prefer", id, new Text(combinedValue), PREFER_BASE_PATH);_
          * Output jsons of individual video response from Google YouTube Data API to a Hive external table
            googleapis_youtube by using the combination of 
              _MultipleOutputs.addNamedOutput(job, "json", TextOutputFormat.class, Text.class, NullWritable.class);_
              _multipleOutputs.write("json", new Text(result.toString()), NullWritable.get(), JSON_BASE_PATH);_
            
        - Join etl_prefer_summary and googleapis_youtube to generate __etl_video_matrix__.
        - topicId is a nested array field of googleapis_youtube table.  Therefore, I had to create 
          googleapis_youtube_exploded table by using LATERAL VIEW explode of googleapis_youtube table
        - Join googleapis_youtube_exploded table with etl_prefer_summary and group by topicid & source to generate 
          __etl_topic_matrix__.           
        - googleapis_youtube table was created using org.openx.data.jsonserde.JsonSerDe and with the schema composed of
          nested Hive STRUCT and ARRAY exactly matching YouTube video response so that I can query googleapis_youtube
          table naturally.     
        - It took time to retrieve video metadata/ statistics via Google YouTube Data API.  I use Secondary Sort 
          technique stated below to ensure I only call the API once for each video.
        - I combined VideoId and Source (SourceOrd 0 for train and 1 for test) into TextIntPair object as the key.   
          I created IdPartitioner (including VideoId only) as the PartitionerClass and GroupComparator (including 
          VideoId only) as the GroupingComparatorClass and KeyComparator (including both) as the SortComparatorClass
          so that all comedy_comparison records of the same video will go to the same Reducer and grouped under 
          the same key.  I will be able to call Google API only once for each video. KeyComparator can be ignore in 
          this case.  Hadoop will  honor the natural order of TextIntPair.
        - I use combiner to aggregate local data and reduce data shuffle between mapper and reducer.    
       
   2. Sort
   
      MaxTemperatureUsingSecondarySort2       
      This is a __Secondary Sort__ example.  The goal is to generate a yearly maximum temperature report using NCDC 
      climate data.
      I used a similar technique as comedy_comparison_etl.  I combined the year and air temerature into an IntPair2 
      object as the key. FirstPartitioner and GroupComparator use year only. The KeyComparator uses the year 
      and air temerature both.  However, airing temporature is in reverse order.  Both Mapper and Reducer use IntPair2 
      as the key and NullWritable as the value.  In another word, Reducer will receive the key combined with the year 
      and the maximum temperature and nothing more and output directly.
      
      SortByTemperatureUsingTotalOrderPartitioner      
      This is a __Total Sort__ example.  The goal is to generate temperature sorted in total order.  By default, 
      MapReduce will sort input records by their keys.  A reduce task with 30 reducers will produce 30 files, each 
      of which is sorted.  There is no easy way to combine the files to produce a globally sorted file.  
      To archive __Total Sort__, one option is to use single partition only.  That defeats the purpose of parallelism.
      The other option is to divide the partition by the key itself.  In this case, that's the temperature.  However, partitions manually divided 
      might cause uneven workload among reducers. SortByTemperatureUsingTotalOrderPartitioner uses
       
      _InputSampler.RandomSampler(freq, numSamples, maximumSplitsSampled)_ 
      
      to sample the key space and save the key distribution. SortByTemperatureUsingTotalOrderPartitioner uses 
      TotalOrderPartitioner as its PartitionClass. TotalOrderPartitioner uses the above key distribution to contruct partitions.
                      
      SortByTemperatureUsingTotalOrderPartitioner uses SortDataPreprocessor to filter out invalid climate data and save 
      to SequenceFile first.
      
   3. Join
   
      MaxTemperatureByStationNameUsingDistributedCacheFile, MaxTemperatureByStationNameUsingDistributedCacheFileApi
      
      Join implementation depends upon how large the dataset to be joined.  We will use DistributedCache if one dataset
      is small enough to be distributed to each node.  MaxTemperatureByStationNameUsingDistributedCacheFile etc.
      distribute NcdcStationMetadata data via DistributedCache then MaxTemperatureReducerWithStationLookup uses it
      for station name lookup.  Please notice that all my applications here use ToolRunner which is backed by 
      GenericOptionsParser which takes hadoop command line parameter _-files_ for files to be distributed. Files can
      be accesssed directly using path.  MaxTemperatureByStationNameUsingDistributedCacheFileApi is a variant to use
      DistributedCache api directly. Hadoop treats files parameters without hdfs:// as local files.
      
      JoinDriver and its associated classes
      
      JoinDriver is an reduce-side join example when both dataset to be joined are large.  JoinDriver uses 
      MultipleInputs as input and TextPair as the key class. MultipleInputs has one input of NCDC metadata processed  
      by JoinStationMapper and another input of NCDC climate records processed by JoinRecordMapper.  JoinStationMapper
      output a TextPair object of stationID combined with "0" tag and JoinRecordMapper output a TextPair object of
      stationID combined with "1" tag.    
       
      JoinDriver uses similar technique we use in SecondarySort. It uses TextPair.first for partitioning as well as 
      GroupComparator.  Since tag "0" comes before tag "1",JoinReducer can retrieve one record of station metadata 
      then loop through NCDC climate data to process.                                                                                                          
   
   4. Split
      
      PartitionByStationYearUsingMultipleOutputs
      
      This is a simple example to illustrate how to output NCDC climate data dynamically in the directory structure of
       {stationID}/{year}.
       
      The key point is to use MultipleOutputs.  It differs from comedy_comparison_etl that I did not pre-configure using
      NamedOutput since the ouput path is dynamic.  The ouput is solely controlled by 
        _String basePath = String.format("%s/%s/part", parser.getStationId(), parser.getYear());_
        _multipleOutputs.write(NullWritable.get(), value, basePath)_
      
      SmallFilesToSequenceFileConverter2 and its associated classes
      
      The goal is to condense lots of small files into SequeneceFile (a binary file of key-value entries.)
      and the file name is the entry key and the content is the entry value.
      
      This is to illustrate using customized InputFormat as well as RecordReader to make an input non-splitable and 
      process the input as a whole.  WholeFileInputFormat2 is the customized InputFormat class. non-splitable input is 
      accomplished by overriding
        _protected boolean isSplitable(JobContext context, Path filename) { return false; }_
        
      WholeFileInputFormat2 also use customized WholeFileRecordReader2 that intitialize FileSplit and Configuration.
        _public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                 fileSplit = (FileSplit) inputSplit;
                 conf = taskAttemptContext.getConfiguration();
             }_
      
      _nextKeyValue()_ is the key method which reads all bytes of the FileSplit and set BytesWritable accordingly so 
      that it reads the file content as a whole.  It has toggled flag and it only return true for the first read.
      The followings are the snippet.
      
              byte[] buf = new byte[(int) fileSplit.getLength()];
              Path path = fileSplit.getPath();         
              FileSystem fs = path.getFileSystem(conf); 
              in = fs.open(path);             
             IOUtils.readFully(in, buf, 0, buf.length);
             value.set(buf, 0, buf.length);         
             
                    
        