package mapreduce;

import org.apache.hadoop.conf.Configuration;
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

public class mapred5 {
    static class Mapper5 extends TableMapper<ImmutableBytesWritable, Text> {

        private Table table;
        private Connection conn;
        private String key = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration hbaseConfig = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(hbaseConfig);
            this.table = conn.getTable(TableName.valueOf("A:C"));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            table.close();

            conn.close();
        }
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String[] splitKey = (new String(row.get())).split("/");
            String year = splitKey[0];
            String ueid = splitKey[1];
            int y = (9999-(Integer.valueOf(year)));
            String y2 = String.valueOf(y);
            String clé = ueid+"/"+y2;
            byte[] bnotes = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("R"));
            String snotes = new String(bnotes);
            Get getValue = new Get(clé.getBytes());

            getValue.addColumn("#".getBytes(), "N".getBytes());

            try {
                Result result = table.get(getValue);
                if (!table.exists(getValue)) {

                    //requested key doesn't exist
                    return;
                }

                byte[] instructor = result.getValue(Bytes.toBytes("I"), Bytes.toBytes("1"));
                String name = Bytes.toString(instructor);
                key = name;

            }
            finally {

            }


            // Read the data

            // emit date and sales values
            context.write(new ImmutableBytesWritable(key.getBytes()), new Text(year+"/"+ueid+"*"+snotes));
        }

    }
    public static class Reducer5 extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String resu = null;
            // loop through different sales vales and add it to sum
            for (Text inputvalue : values) {

               resu = inputvalue.toString();
            }


            Put insHBase = new Put(key.get());
            // insert sum value to hbase

            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("R"), Bytes.toBytes(resu));
            // write data to Hbase table
            context.write(null, insHBase);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        TableUtil.createTableIfNotExists(connection, "21402752Q6", "#");

        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred5.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "21402752Q4",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred5.Mapper5.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q6",      // output table
                mapred5.Reducer5.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
