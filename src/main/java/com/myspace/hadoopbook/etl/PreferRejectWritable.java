package com.myspace.hadoopbook.etl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/23/14
 * Time: 9:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class PreferRejectWritable implements Writable {

    private int leftPreferred;
    private int rightPreferred;
    private int leftRejected;
    private int rightRejected;
    private int sourceOrd;


    public PreferRejectWritable() {
    }

    public PreferRejectWritable(int sourceOrd) {
        this.sourceOrd = sourceOrd;
    }


    public PreferRejectWritable(int leftPreferred, int rightPreferred, int leftRejected, int rightRejected, int sourceOrd) {
        this.leftPreferred = leftPreferred;
        this.rightPreferred = rightPreferred;
        this.leftRejected = leftRejected;
        this.rightRejected = rightRejected;
        this.sourceOrd = sourceOrd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(leftPreferred);
        out.writeInt(rightPreferred);
        out.writeInt(leftRejected);
        out.writeInt(rightRejected);
        out.writeInt(sourceOrd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        leftPreferred = in.readInt();
        rightPreferred = in.readInt();
        leftRejected = in.readInt();
        rightRejected = in.readInt();
        sourceOrd = in.readInt();
    }

    public void incLeftPreferred() {
        leftPreferred++;
    }

    public void incRightPreferred() {
        rightPreferred++;
    }

    public void incLeftRejected() {
        leftRejected++;
    }

    public void incRightRejected() {
        rightRejected++;
    }

    public void incrementBy(PreferRejectWritable in) {
        this.leftPreferred += in.getLeftPreferred();
        this.rightPreferred += in.getRightPreferred();
        this.leftRejected += in.getLeftRejected();
        this.rightRejected += in.getRightRejected();
    }

    public int getLeftPreferred() {
        return leftPreferred;
    }

    public int getRightPreferred() {
        return rightPreferred;
    }

    public int getLeftRejected() {
        return leftRejected;
    }

    public int getRightRejected() {
        return rightRejected;
    }

    public int getSourceOrd() {
        return sourceOrd;
    }

    public PreferRejectWritable clone() {
       return  new PreferRejectWritable(this.leftPreferred, this.rightPreferred, this.leftRejected, this.rightRejected, this.sourceOrd);
    }

    @Override
    public String toString() {
        return "PreferRejectWritable{" +
                "leftPreferred=" + leftPreferred +
                ", rightPreferred=" + rightPreferred +
                ", leftRejected=" + leftRejected +
                ", rightRejected=" + rightRejected +
                ", sourceOrd=" + sourceOrd +
                '}';
    }

    public enum PreferenceSource {
        TRAIN,
        TEST;

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }


}
