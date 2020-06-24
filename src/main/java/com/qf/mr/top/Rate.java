package com.qf.mr.top;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Rate implements WritableComparable<Rate> {
    private String movie;
    private String rate;
    private String timeStamp;
    private String uid;

    public Rate() {
    }

    @Override
    public String toString() {
        return movie + "\t" +
               rate + "\t" +
               timeStamp + "\t" +
               uid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rate rate1 = (Rate) o;

        if (movie != null ? !movie.equals(rate1.movie) : rate1.movie != null) return false;
        if (rate != null ? !rate.equals(rate1.rate) : rate1.rate != null) return false;
        if (timeStamp != null ? !timeStamp.equals(rate1.timeStamp) : rate1.timeStamp != null) return false;
        return uid != null ? uid.equals(rate1.uid) : rate1.uid == null;
    }

    @Override
    public int hashCode() {
        int result = movie != null ? movie.hashCode() : 0;
        result = 31 * result + (rate != null ? rate.hashCode() : 0);
        result = 31 * result + (timeStamp != null ? timeStamp.hashCode() : 0);
        result = 31 * result + (uid != null ? uid.hashCode() : 0);
        return result;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public int compareTo(Rate o) {
        /*int i = this.rate.compareTo(o.getRate());
        if(i == 0){
            i = this.uid.compareTo(o.getUid());
        }*/
        //return  i;
        int compare = Integer.compare(Integer.parseInt(this.rate), Integer.parseInt(o.getRate()));
        return compare == 0 ? 1 : compare;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movie);
        out.writeUTF(rate);
        out.writeUTF(timeStamp);
        out.writeUTF(uid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movie = in.readUTF();
        rate = in.readUTF();
        timeStamp = in.readUTF();
        uid = in.readUTF();
    }
}
