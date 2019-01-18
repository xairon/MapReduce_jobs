package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class mapred3 {
    static class Mapper3 extends TableMapper<ImmutableBytesWritable, IntWritable> {

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
            String name = null;
            // get rowKey and convert it to string
            String inKey = new String(row.get());
            // set new key having only date
            String oKey = inKey.split("/")[0];
            String oKey2 = inKey.split("/")[2];
            String keymoyenne = oKey2+"/"+Integer.toString(9999-Integer.valueOf(oKey));
            // get sales column in byte format first and then convert it to
            // string (as it is stored as string from hbase shell)
            byte[] bnotes = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
            String snotes = new String(bnotes);
            Get getValue = new Get(keymoyenne.getBytes());

            getValue.addColumn("#".getBytes(), "N".getBytes());

            try {
                Result result = table.get(getValue);
                if (!table.exists(getValue)) {

                    //requested key doesn't exist
                    return;
                }

                byte[] nom = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
                name = Bytes.toString(nom);
                key = oKey+"/"+name+"/"+oKey2;

            }
            finally {

            }


            // Read the data

            // emit date and sales values
            context.write(new ImmutableBytesWritable(key.getBytes()), new IntWritable(Integer.valueOf(snotes)));
        }

    }
    public static class Reducer3 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] splitKey = (new String(key.get())).split("/");
            String year = splitKey[0];
            String uname = splitKey[1];
            String uid = splitKey[2];
            String clé = year+"/"+uid;
            int sum = 0;
            int compteur = 0;
            // loop through different sales vales and add it to sum
            for (IntWritable inputvalue : values) {

                sum += inputvalue.get();
                compteur++;
            }
            double moyenne = ((double)sum/(double)compteur)/100.0;
            String smoyenne = String.valueOf(moyenne);
            System.out.println(moyenne);
            // create hbase put with rowkey as date

            Put insHBase = new Put(clé.getBytes());
            // insert sum value to hbase
            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"), Bytes.toBytes(uname+"/"+smoyenne));
            // write data to Hbase table
            context.write(null, insHBase);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        TableUtil.createTableIfNotExists(connection, "21402752Q4", "#");

        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(mapred3.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred3.Mapper3.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q4",      // output table
                mapred3.Reducer3.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
