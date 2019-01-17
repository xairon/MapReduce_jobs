package mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;


public class mapred1 {

    static class Mapper1 extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
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

            String triplet = year + "/" + student + "/" + new String(bnotes);

            context.write(new ImmutableBytesWritable(semester.getBytes()),
                    new ImmutableBytesWritable(triplet.getBytes()));
        }


    }

    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context){

            String semester = new String(key.get());
            HashMap<String, HashMap<String, List<Double>>> classes = new HashMap<>();

            for (ImmutableBytesWritable value : values){
                String[] decode = (new String(value.get())).split("/");
                String year = decode[0]; String student = decode[1];
                double grade = Integer.parseInt(decode[3])/100.0;
                classes.putIfAbsent(year, new HashMap<>());
                HashMap<String, List<Double>> students = classes.get(year);
                students.putIfAbsent(student, new ArrayList<>());
                students.get(student).add(grade);
            }


            classes.forEach((year, students) -> {
                Counter all = new Counter(0);
                Counter passed = new Counter(0);
                students.forEach((student, grades) -> {
                    all.increment();
                    if (grades.stream().mapToDouble(Double::doubleValue).sum()/grades.size() >= 10d)
                        passed.increment();
                });
                double taux = ((double)passed.get())/((double)passed.get());
                String clef = semester + "/" + year;
                Put insHBase = new Put(clef.getBytes());
                insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("S"), Bytes.toBytes(taux));
                try {
                    context.write(null, insHBase);
                }catch (InterruptedException | IOException ignored){}

            });

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred1.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred1.Mapper1.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                ImmutableBytesWritable.class,             // mapper output value
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