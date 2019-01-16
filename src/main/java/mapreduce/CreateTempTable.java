package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CreateTempTable {
    static class MapperTemp extends TableMapper<ImmutableBytesWritable, Text> {

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
            String name;
            // get rowKey and convert it to string
            String inKey = new String(row.get());

            String[] splitted = inKey.split("/");
            String year = splitted[0];
            String yearInvert = String.valueOf(9999-Integer.valueOf(year));

            String sem = splitted[1].substring(0,2);
            String etu = splitted[1].substring(2,12);
            String ue = splitted[2];

            String courseKey = ue+"/"+yearInvert;


            Result result;
            try {
                Scan firstUEScanner = new Scan();
                firstUEScanner.withStartRow(courseKey.getBytes());
                firstUEScanner.setMaxResultSize(1);
                firstUEScanner.setCacheBlocks(false);

                ResultScanner resultScanner = table.getScanner(firstUEScanner);
                result = resultScanner.next();


                if (result == null || !result.getExists()) {
                    System.out.println("key doesn't exists (CreateTempTable): " + courseKey);
                    //requested key doesn't exist
                    return;
                }


                byte[] bytes = result.getValue("#".getBytes(), "N".getBytes());
                String ueName = new String(bytes);


                String strValue = new String(value.value());
                int grade = Integer.valueOf(strValue);

                String key = year+"/"+sem+"/"+etu;
                context.write(
                        new ImmutableBytesWritable(key.getBytes()),
                        new Text(ue+"/"+ueName+"/"+grade));


            }
            catch (HBaseIOException e){
                e.printStackTrace();
                System.err.println("An error occured in CreateTempTable Mapper");
            }
        }

    }
    public static class ReducerTemp extends TableReducer<ImmutableBytesWritable, Pair<String,Integer>, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Put put = new Put(key.get());
            for (Text inputvalue : values) {
                String[] splitted = (new String(inputvalue.getBytes())).split("/");
                String columnName = splitted[0]+"/"+splitted[1];
                put.addColumn("#".getBytes(), columnName.getBytes(), splitted[2].getBytes());

            }

            context.write(null, put);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("21402752_Temp");

        if (!connection.getAdmin().tableExists(tableName)){
            System.out.println("temp table does not exists, beginning table creation...");
            System.out.println("creating temp table descriptor...");

            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            System.out.println("Adding column family...");
            hTableDescriptor.addFamily(new HColumnDescriptor("#".getBytes()));
            System.out.println("creating table from table descriptor...");
            connection.getAdmin().createTable(hTableDescriptor);
            System.out.println("finished creating temp table");
        }

        Job job = Job.getInstance(config, "TestconfigMapper");
        job.setJarByClass(MapperTemp.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                CreateTempTable.MapperTemp.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                IntWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752_Temp",      // output table
                CreateTempTable.ReducerTemp.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }


    }
}
