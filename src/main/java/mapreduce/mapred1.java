package mapreduce;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;


public class mapred1 {

    public static class UEGrade {
        public final String UE;
        public final int grade;

        public UEGrade(String UE, int grade) {
            this.UE = UE;
            this.grade = grade;
        }
    }

    static class Mapper1 extends TableMapper<ImmutableBytesWritable, UEGrade> {
        private static final IntWritable one = new IntWritable(1);

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // get rowKey and convert it to string
            String inKey = new String(row.get());
            // set new key having only date
            String[] splitted = inKey.split("/");

            String year = splitted[0];

            String semester = splitted[1].substring(0,2);
            String student = splitted[1].substring(2,12);

            byte[] bnotes = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
            String snotes = new String(bnotes);

            String yearSemEtu = year+"/"+semester+"/"+student;

            context.write(new ImmutableBytesWritable(yearSemEtu.getBytes()),
                    new UEGrade(splitted[2], Integer.valueOf(snotes)));
        }


    }

    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, UEGrade, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<UEGrade> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int compteur = 0;

            HashMap<String,Integer> ueGradeMap = new HashMap<>();
            HashMap<String,Integer> ueGradeCount = new HashMap<>();

            for (UEGrade ueGrade: values){
                sum += ueGrade.grade;
                compteur++;

                ueGradeMap.putIfAbsent(ueGrade.UE, 0);
                ueGradeMap.compute(ueGrade.UE,(k,v) -> v + ueGrade.grade);

                ueGradeCount.putIfAbsent(ueGrade.UE, 0);
                ueGradeCount.compute(ueGrade.UE, (k,v) -> v+1);
            }


            double moyenne = ((double)sum/(double)compteur);
            String smoyenne = String.valueOf(moyenne);
            System.out.println(smoyenne);


            Put insHBase = new Put(key.get());

            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("G"), Bytes.toBytes(smoyenne));
            // write data to Hbase table
            context.write(null, insHBase);


        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred1.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred1.Mapper1.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q2",      // output table
                mapred1.Reducer1.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}