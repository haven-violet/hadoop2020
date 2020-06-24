package com.qf.mr.top;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RateReducer extends Reducer<Rate, NullWritable, Rate, NullWritable> {

    @Override
    protected void reduce(Rate key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
