package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class mapred3 {
    static class Mapper3 extends TableMapper<ImmutableBytesWritable, Text> {

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

                String outKey = year+"/"+ue;
                String outValue = ueName+"/"+(Double.valueOf(str_grade)/100.0);

                context.write(
                        new ImmutableBytesWritable(outKey.getBytes()),
                        new Text(outValue));

            }

        }

    }
    public static class Reducer3 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {


            int sum = 0;
            int compteur = 0;
            // loop through different sales vales and add it to sum
            for (IntWritable inputvalue : values) {
                double a = Double.valueOf(inputvalue.get())/100.0;
                if(a>=10)
                {sum++;}
                compteur++;
            }
            double moyenne = ((double)sum/(double)compteur);
            String smoyenne = String.valueOf(moyenne);
            System.out.println(smoyenne);
            // create hbase put with rowkey as date

            Put insHBase = new Put(key.get());
            // insert sum value to hbase
            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("R"), Bytes.toBytes(smoyenne));
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
