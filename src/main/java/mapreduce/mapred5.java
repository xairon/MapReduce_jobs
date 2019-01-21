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

public class mapred5 {
    static class Mapper5 extends TableMapper<ImmutableBytesWritable, Text> {

        private Table table;
        private Connection conn;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration hbaseConfig = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(hbaseConfig);
            this.table = conn.getTable(TableName.valueOf("21402752Q4"));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            table.close();

            conn.close();
        }
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String[] splitKey = (new String(row.get())).split("/");
            String year = splitKey[1];
            String ueid = splitKey[0];
            int y = (9999-(Integer.valueOf(year)));
            String y2 = String.valueOf(y);
            String clé = y2+"/"+ueid;
            Get get = new Get(clé.getBytes());
            get.addColumn("#".getBytes(), "R".getBytes());
            Result result = table.get(get);

            if (result == null){

                return;
            }
            String[] splitrate;
            String rate = new String();
            String uename = new String();

            byte[]valuerate = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("R"));


            String valueR = Bytes.toString(valuerate);

            if(valueR!=null){
                splitrate = valueR.split("/");
                uename = splitrate[0];
                rate = splitrate[1];
            }
            System.out.println(uename);
            System.out.println(rate);





            for (Cell cell: value.listCells()) {

                String instructor = Bytes.toString(CellUtil.cloneValue(cell));
               String outKey = instructor+"/"+year;

                String Outvalue = ueid+"/"+uename+"/"+rate;


                context.write(
                        new ImmutableBytesWritable(outKey.getBytes()),
                        new Text(Outvalue));

            }



        }

    }
    public static class Reducer5 extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String[] splitKey = (new String(key.get())).split("/");
            String intervenant = splitKey[0];
            String year = splitKey[1];
            String Outvalue = new String();
            String clé = intervenant;
            for(Text text : values){
                String[] splittedValue = Bytes.toString(text.copyBytes()).split("/");
                String ueid = splittedValue[0];
                String ueName = splittedValue[1];
                String rate = splittedValue[2];
                Outvalue = ueid+"/"+year+"/"+ueName+"/"+rate;
            }

            // create hbase put with rowkey as date

            Put insHBase = new Put(clé.getBytes());
            // insert sum value to hbase
            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("R"), Bytes.toBytes(Outvalue));
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
                "A:C",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred5.Mapper5.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                Text.class,
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
