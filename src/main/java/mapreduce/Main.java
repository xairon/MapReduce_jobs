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
        Main.main(args);
        mapred1.main(args);
        mapred2.main(args);
        mapred3.main(args);
        mapred4.main(args);
        mapred5.main(args);
        mapred6.main(args);
    }
}
