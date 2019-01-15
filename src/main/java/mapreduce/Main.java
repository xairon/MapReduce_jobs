package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred1.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        Job job2 = Job.getInstance(config, "TestconfigMapper");
        job2.setJarByClass(mapred2.class);
        Scan scan2 = new Scan();
        scan2.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan2.setCacheBlocks(false);  // don't set to true for MR jobs
        Job job3 = Job.getInstance(config, "TestconfigMapper");
        job3.setJarByClass(mapred3.class);
        Scan scan3 = new Scan();
        scan3.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan3.setCacheBlocks(false);  // don't set to true for MR jobs
        Job job4 = Job.getInstance(config, "TestconfigMapper");
        job4.setJarByClass(mapred4.class);
        Scan scan4 = new Scan();
        scan4.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan4.setCacheBlocks(false);  // don't set to true for MR jobs
        Job job5 = Job.getInstance(config, "TestconfigMapper");
        job5.setJarByClass(mapred5.class);
        Scan scan5 = new Scan();
        scan5.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan5.setCacheBlocks(false);  // don't set to true for MR jobs
        Job job6 = Job.getInstance(config, "TestconfigMapper");
        job6.setJarByClass(mapred6.class);
        Scan scan6 = new Scan();
        scan6.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan6.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred1.Mapper1.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q2",      // output table
                mapred1.Reducer1.class,  // reducer class
                job);
        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan2,             // Scan instance to control CF and attribute selection
                mapred2.Mapper2.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job2);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q3",      // output table
                mapred2.Reducer2.class,  // reducer class
                job2);
        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan3,             // Scan instance to control CF and attribute selection
                mapred3.Mapper3.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job3);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q4",      // output table
                mapred3.Reducer3.class,  // reducer class
                job3);
        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan4,             // Scan instance to control CF and attribute selection
                mapred4.Mapper4.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job4);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q5",      // output table
                mapred4.Reducer4.class,  // reducer class
                job4);
        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan5,             // Scan instance to control CF and attribute selection
                mapred5.Mapper5.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job5);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q6",      // output table
                mapred5.Reducer5.class,  // reducer class
                job5);
        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred6.Mapper6.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job6);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q7",      // output table
                mapred6.Reducer6.class,  // reducer class
                job6);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job 1!");
        }
        boolean b2 = job2.waitForCompletion(true);
        if (!b2) {
            throw new IOException("error with job 2!");
        }
        boolean b3 = job3.waitForCompletion(true);
        if (!b3) {
            throw new IOException("error with job 3!");
        }
        boolean b4 = job4.waitForCompletion(true);
        if (!b4) {
            throw new IOException("error with job 4!");
        }
        boolean b5 = job5.waitForCompletion(true);
        if (!b5) {
            throw new IOException("error with job 5!");
        }
        boolean b6 = job6.waitForCompletion(true);
        if (!b6) {
            throw new IOException("error with job 6!");
        }
    }
}
