package mapreduce;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class TableUtil {

    /**
     * @param connection
     * @param name
     * @param columnFamilies
     * @return true if table was dropped
     * @throws IOException
     */
    public static boolean createTableIfNotExists(Connection connection, String name, String[] columnFamilies) throws IOException {
        TableName tableName = TableName.valueOf(name);

        if (!connection.getAdmin().tableExists(tableName)){
            System.out.println("table "+name+" does not exists, beginning table creation...");
            System.out.println("creating temp table descriptor...");

            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            System.out.println("Adding column family...");

            for (int i = 0; i < columnFamilies.length; i++) {
                System.out.println("Creating column family: "+columnFamilies[i]);
                hTableDescriptor.addFamily(new HColumnDescriptor(columnFamilies[i].getBytes()));
            }

            System.out.println("creating table from table descriptor...");
            connection.getAdmin().createTable(hTableDescriptor);
            System.out.println("finished creating table: "+name);

            return true;
        }
        return false;
    }

    /**
     *
     * @param connection
     * @param name
     * @return true if table is deleted
     * @throws IOException
     */
    public static boolean dropTableIfExists(Connection connection, String name) throws IOException {
        TableName tableName = TableName.valueOf(name);

        Admin admin = connection.getAdmin();

        if (admin.tableExists(tableName)){
            System.out.println("table "+name+" does exists, beginning table suppression...");
            admin.deleteTable(tableName);
            System.out.println("table "+name+" deleted.");
            return true;
        }
        else {
            System.out.println("INFO: table "+name+" does not exists.");
        }
        return false;
    }

    public static void dropAllTables(Connection connection, String prefix) throws IOException {
        Admin admin = connection.getAdmin();

        System.out.println("Deleting tables with prefix "+prefix);
        admin.deleteTables(prefix+".*");
        System.out.println("Finished deleting tables.");
    }


}
