package com.myspace.hadoopbook.etl;

import com.myspace.hadoopbook.TextIntPair;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/24/14
 * Time: 10:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class EtlCombiner extends Reducer<TextIntPair, PreferRejectWritable, TextIntPair, PreferRejectWritable> {
    //Combiner is a local aggregator.
    //According to the definition, the Combiner may be called 0, 1, or many times on each key between the mapper and reducer.
    //Therefore, I should not assume anything
    //The input of combiner can come from multiple mappers in the same node.  Therefore, there can be multiple sources (train/ test)


    @Override
    protected void reduce(TextIntPair key, Iterable<PreferRejectWritable> values, Context context) throws IOException, InterruptedException {

        int currSourceOrd = -1;
        PreferRejectWritable currWritable = null;

        for (PreferRejectWritable value: values) {
            if (value.getSourceOrd() != currSourceOrd ) {
              if (currWritable != null)
                  context.write(new TextIntPair(key.getId(), currSourceOrd), currWritable);
              currSourceOrd = value.getSourceOrd();
              currWritable = value.clone();
            }
            else currWritable.incrementBy(value);
        }
        //Write out the last batch but prevent empty iterator too
        if (currWritable != null)
            context.write(new TextIntPair(key.getId(), currSourceOrd), currWritable);
    }


}
