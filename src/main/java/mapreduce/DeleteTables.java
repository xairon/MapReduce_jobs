package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteTables {


    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(configuration);

        TableUtil.dropAllTables(connection, "21402752");


    }


}
