package com.qf.mr.depend;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

/**
 * 定义一个主类,用来调用多个job,并且设置它们之间的关系
 */
public class DependTool {
    public static void main(String[] args) throws IOException, InterruptedException {
        //需求: 先完成宠物的序列化排序,把结果输出(CatDriver),然后统计上一步输出结果的单词数量(WordCountDriver)

        //先通过工厂模式完成Cat,WordCount,Job的构建
        Job job1 = JobFactory.getCatJob();
        Job job2 = JobFactory.getWordCountJob();
        Job job3 = JobFactory.getWordCountJob1();

        //定义ControlledJob,用来设定Job之间依赖关系,并且构造器传入要包装的job的conf
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());

        //对ControlledJob进行原生的Job的设置
        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        controlledJob3.setJob(job3);

        //设置两个Job之间的依赖关系
        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);

        //定义JobControl并且把新建的ControlledJob加入,类似于组的概念
        JobControl jobControl = new JobControl("QFJobControl");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);

        //把JobControl传入到多线程的构建函数中,用多线程的方式启动JobControlled
        Thread thread = new Thread(jobControl);

        //通过启动多线程启动多个job
        thread.start();

        //等待所有线程都完成后在停止JobControl
        while(!jobControl.allFinished()){
            Thread.sleep(800);
        }
        //关闭当前jobControl
        jobControl.stop();
    }
}
