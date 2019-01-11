package mapreduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
public class mapred2 {


        static class Mapper2 extends TableMapper<Text, Text> {


            public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

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
                Configuration c = HBaseConfiguration.create();      // Instantiate Configuration class
                HTable table = new HTable(c, "A:C"); // Instantiate HTable class
                Get g = new Get(Bytes.toBytes(keymoyenne));        // Instantiate Get class
                Result result = table.get(g);      // Read the data
                byte [] nom = result.getValue(Bytes.toBytes("#"),Bytes.toBytes("N"));
                String name = Bytes.toString(nom);      // Print the values

                // emit date and sales values
                context.write(new Text(name+"/"+oKey2), new Text(snotes));
            }
        }
            public static void main(String[] args) throws Exception {
                Configuration config = HBaseConfiguration.create();
                Job job = Job.getInstance(config, "TestconfigMapper");
                job.setJarByClass(mapred2.class);
                Scan scan = new Scan();
                scan.setCaching(500000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
                scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

                TableMapReduceUtil.initTableMapperJob(
                        "A:G",      // input table
                        scan,             // Scan instance to control CF and attribute selection
                        mapred2.Mapper2.class,   // mapper class
                        Text.class,             // mapper output key
                        Text.class,             // mapper output value
                        job);
                TableMapReduceUtil.initTableReducerJob(
                        "21402752Q3",      // output table
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
