package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MainMap {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred1.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:S",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred1.Mapper1.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q2",      // output table
                mapred1.Reducer1.class,  // reducer class
                job);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
