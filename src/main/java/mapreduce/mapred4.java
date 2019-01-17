package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class mapred4 {
    static class Mapper4 extends TableMapper<ImmutableBytesWritable, Text> {

        private Table table;
        private Connection conn;
        private String key = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration hbaseConfig = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(hbaseConfig);
            this.table = conn.getTable(TableName.valueOf("A:S"));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            table.close();

            conn.close();
        }
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

            String[] splitKey = (new String(row.get())).split("/");
            String year = splitKey[0];
            String sem = splitKey[1];
            String etu = splitKey[2];



            Get get = new Get(etu.getBytes());
            get.addFamily("#".getBytes());

            Result result = table.get(get);

            if (result == null){
                System.err.println("Student not found in Student table: "+etu);
                return;
            }

            String program = Bytes.toString(result.getValue("#".getBytes(), "P".getBytes()));

            for (Cell cell: value.listCells()) {
                String[] splittedUE = Bytes.toString(CellUtil.cloneQualifier(cell)).split("/");
                String ue = splittedUE[0];
                String ueName = splittedUE[1];
                String str_grade = Bytes.toString(CellUtil.cloneValue(cell));

                String outKey = program+"/"+year;
                String outValue = ue+"/"+ueName+"/"+(Double.valueOf(str_grade)/100.0);

                context.write(
                        new ImmutableBytesWritable(outKey.getBytes()),
                        new Text(outKey));

            }

        }

    }
    public static class Reducer4 extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            String[] splittedKey = Bytes.toString(key.get()).split("/");
            String program = splittedKey[0];
            String year = splittedKey[1];

            HashMap<String, Double> sums = new HashMap<>();
            HashMap<String, Integer> counts = new HashMap<>();

            for (Text text: values) {

                String[] splittedValue = Bytes.toString(text.copyBytes()).split("/");
                String ue = splittedValue[0];
                String ueName = splittedValue[1];

                double grade_ = 0;

                try {
                    grade_ = Double.valueOf(splittedValue[2]);
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    System.err.println("Error with key "+ Arrays.toString(splittedValue));
                }

                double grade = grade_;

                String ueComp = ue+"/"+ueName;

                sums.putIfAbsent(ueComp, 0.0);
                counts.putIfAbsent(ueComp, 0);

                sums.compute(ueComp, (k,v) -> v+grade );
                counts.compute(ueComp, (k,v) -> v+1 );

            }

            Put insHBase = new Put(key.get());

            for (Map.Entry<String,Double> entry : sums.entrySet()) {
                double avgGrade = entry.getValue()/((double) counts.get(entry.getKey()));

                insHBase.addColumn(
                        Bytes.toBytes("#"),
                        Bytes.toBytes(entry.getKey()),
                        Bytes.toBytes(String.valueOf(avgGrade))
                );

            }

            // write data to Hbase table
            context.write(null, insHBase);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        TableUtil.createTableIfNotExists(connection, "21402752Q5", "#");

        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred4.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "21402752_Temp",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred4.Mapper4.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q5",      // output table
                mapred4.Reducer4.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
