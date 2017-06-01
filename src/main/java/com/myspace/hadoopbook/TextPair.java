package com.myspace.hadoopbook;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 7/4/13
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */

//get from http://haloop.googlecode.com/svn-history/r328/trunk/src/examples/org/apache/hadoop/examples/textpair/TextPair.java
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(Text t1, Text t2) {
        set(t1, t2);
    }

    public TextPair(String first,String second){
        set(new Text(first), new Text(second));
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public void readFields(DataInput in) throws IOException {

        first.readFields(in);
        second.readFields(in);
    }

    //Partitioner would be responsible for which reducer map records would go to, In the case of reduce-side join, we want to use customized one
    //to ensure records from both stationName source and weather record source would go to the same one if their stationId is the same.
    //job. job.setGroupingComparatorClass() would regulate which map records would go to the same reducer group (treated with one key and multiple values)
    public int compareTo(TextPair tp) {
        int cmp =  getFirst().compareTo(tp.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return getSecond().compareTo(tp.getSecond());
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    public boolean equals(Object o) {
        TextPair p = (TextPair) o;
        return first.equals(p.getFirst()) && second.equals(p.getSecond());
    }

    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair)
                return ((TextPair)a).getFirst().compareTo(((TextPair)b).getFirst());
            return super.compare(a, b);    //To change body of overridden methods use File | Settings | File Templates.
        }
    }

}

