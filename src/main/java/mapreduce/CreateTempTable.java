package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CreateTempTable {
    static class MapperTemp extends TableMapper<ImmutableBytesWritable, IntWritable> {

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


            try {
                Scan firstUEScanner = new Scan();
                firstUEScanner.withStartRow(keymoyenne.getBytes());
                firstUEScanner.setMaxResultSize(1);
                firstUEScanner.setCacheBlocks(false);

                ResultScanner resultScanner = table.getScanner(firstUEScanner);
                Result result = resultScanner.next();


                if (result == null) {
                    System.out.println("key doesn't exists (Exo3): "+keymoyenne);
                    //requested key doesn't exist
                    return;
                }

                byte[] nom = result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
                name = Bytes.toString(nom);
                key = name+"/"+oKey2+"/"+oKey;

            }
            catch (Exception e) {
                e.printStackTrace();
                System.out.println("Erreur Exo3");
            }


            // Read the data

            // emit date and sales values
            context.write(new ImmutableBytesWritable(key.getBytes()), new IntWritable(Integer.valueOf(snotes)));
        }

    }
    public static class ReducerTemp extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {


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

            Put insHBase = new Put(key.get());
            // insert sum value to hbase
            insHBase.addColumn(Bytes.toBytes("#"), Bytes.toBytes("G"), Bytes.toBytes(smoyenne));
            // write data to Hbase table
            context.write(null, insHBase);

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("creating temp table descriptor...");

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("21402752_Temp"));
        System.out.println("Adding column family...");
        hTableDescriptor.addFamily(HColumnDescriptor.parseFrom("A".getBytes()));
        System.out.println("creating table from table descriptor...");
        connection.getAdmin().createTable(hTableDescriptor);
        System.out.println("finished creating temp table");


    }
}
