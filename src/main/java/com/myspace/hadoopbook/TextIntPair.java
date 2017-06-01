package com.myspace.hadoopbook;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sonyaling
 * Date: 1/25/14
 * Time: 9:05 AM
 * To change this template use File | Settings | File Templates.
 * Any type which is to be used as a key in the Hadoop Map-Reduce framework should implement this interface
 * WritableComparable
 */
public class TextIntPair implements WritableComparable<TextIntPair> {
    private Text id;
    private int order;

    //Need default constructor for WritableComparable to work
    public TextIntPair() {
        set(new Text(), -1);
    }

    public TextIntPair(Text id, int order) {
       set(id, order);
    }

    public TextIntPair(String idStr, int order){
        set(new Text(idStr), order);
    }

    public void set(Text id, int order) {
        this.id = id;
        this.order = order;
    }

    public Text getId() {
        return id;
    }

    public int getOrder() {
        return order;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        out.writeInt(order);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        order = in.readInt();
    }

    @Override
    public int compareTo(TextIntPair o) {
        int cmp =  getId().compareTo(o.getId());
        if (cmp != 0) {
            return cmp;
        }
        return compare(getOrder(), o.getOrder());
    }

    public static int compare(int a, int b) {
        return (a < b) ? -1 : (a == b ? 0 : 1);
    }

    @Override
    public String toString() {
        return id + "\t" + order;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextIntPair) {
            TextIntPair ip = (TextIntPair)o;
            return id.equals(ip.getId()) && order == ip.getOrder();
        }

        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode() * 163 + order;
    }


}
