package com.qf.hive.function.home;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * 自定义一个UDAF自定义函数,默认要继承UDAF
 */
public class UDAFSum_Sample extends UDAF {
    public static class Evaluator implements UDAFEvaluator {
        private boolean mEmpty;
        private int mSum;
        public Evaluator() {
            super();
            init();
        }
        @Override
        public void init() {
            mSum = 0;
            mEmpty = true;
        }

        public boolean iterate(IntWritable o) {
            if (o != null) {
                mSum += o.get();
                mEmpty = false;
            }
            return true;
        }

        public IntWritable terminatePartial() {
            // This is SQL standard - sum of zero items should be null.
            return mEmpty ? null : new IntWritable(mSum);
        }

        public boolean merge(IntWritable o) {
            if (o != null) {
                mSum += o.get();
                mEmpty = false;
            }
            return true;
        }

        public IntWritable terminate() {
            // This is SQL standard - sum of zero items should be null.
            return mEmpty ? null : new IntWritable(mSum);
        }
    }
}