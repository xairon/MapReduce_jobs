package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.*;


public class mapred6 {
    static class Mapper6 extends TableMapper<ImmutableBytesWritable, Text> {

        private Table table;
        private Connection conn;


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

            // get rowKey and convert it to string
            String inKey = new String(row.get());

            String[] splitted = inKey.split("/");
            String year = splitted[0];
            String etu = splitted[1].substring(2,12);

            Result result;
            try {
                Scan firstUEScanner = new Scan();
                firstUEScanner.withStartRow(etu.getBytes());
                firstUEScanner.setMaxResultSize(1);
                firstUEScanner.setCacheBlocks(false);

                ResultScanner resultScanner = table.getScanner(firstUEScanner);
                result = resultScanner.next();

                if ((result == null)) {
                    System.out.println("key doesn't exists (mapred6): " + etu);
                    //requested key doesn't exist
                    return;
                }

                byte[] bytes = result.getValue("#".getBytes(), "P".getBytes());
                String program = new String(bytes);

                String strValue = new String(value.value());
                int grade = Integer.valueOf(strValue);

                String key = program+"/"+year;

                String outvalue = etu+"/"+grade;

                context.write(
                        new ImmutableBytesWritable(key.getBytes()),
                        new Text(outvalue));


            }
            catch (HBaseIOException e){
                e.printStackTrace();
                System.err.println("An error occurred in mapred6 Mapper");
            }
        }

    }

    public static class Reducer6 extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList <Pair<String, Double>>list = new ArrayList<>();
            HashMap<String,Integer> sum= new HashMap<>();
            HashMap<String,Double>listnote = new HashMap<>();




            for (Text value : values){
                String[] decode = value.toString().split("/");
                final String student = decode[0];
                double grade = Double.parseDouble(decode[1])/100.0;
                listnote.putIfAbsent(student, 0.0);
                sum.putIfAbsent(student, 0);
                listnote.compute(student, (k,v) -> v+grade );
                sum.compute(student, (k,v) -> v+1 );

            }
            Put insHBase = new Put(key.get());
            for (Map.Entry<String,Double> entry : listnote.entrySet()) {
                double avgGrade = entry.getValue()/((double) sum.get(entry.getKey()));

                if(entry.getKey()!=null)
                list.add(new Pair<>(entry.getKey(), avgGrade));
            }


            list.sort((x,y) -> {
              if (x.b<y.b) {
                  return 1;
              }
              else if (x.b>y.b){
                  return -1;
              }
              return 0;
          });



           for(int i = 0; i<list.size();i++){
               insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes(i), Bytes.toBytes(list.get(i).a+"/"+list.get(i).b.toString()));

           }
            try {
                context.write(null, insHBase);
            }catch (InterruptedException | IOException ignored){}


        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        TableUtil.createTableIfNotExists(connection, "21402752Q7", "#");

        Job job = Job.getInstance(config, "Q7Mapper");
        job.setJarByClass(mapred6.class);
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                "A:G",      // input table
                scan,             // Scan instance to control CF and attribute selection
                mapred6.Mapper6.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "21402752Q7",      // output table
                mapred6.Reducer6.class,  // reducer class
                job);
        //job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        //job.setMapOutputValueClass(Put.class);


        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
