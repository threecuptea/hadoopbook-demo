package com.myspace.hadoopbook;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 12/11/13
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class IntPair2 implements WritableComparable<IntPair> {


    private int first;
    private int second;

    public IntPair2() {
    }


    public IntPair2(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair o) {
        int cmp = compare(this.first, o.getFirst());
        if (cmp == 0) {
            cmp = compare(this.second, o.getSecond());
        }
        return cmp;
    }

    public static int compare(int a, int b) {
        return (a < b) ? -1 : ( a== b ? 0 : 1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntPair2 intPair2 = (IntPair2) o;

        if (first != intPair2.first) return false;
        if (second != intPair2.second) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }
}
