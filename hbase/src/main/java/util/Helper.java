package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Helper utils to carry out Admin and CRUD operations in HBase.
 */

public class Helper {

    private Configuration conf;
    private HBaseAdmin hBaseAdmin;

    /**
     * Construct the HBase helper with the specified configuration.
     * @param conf Configuration
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     */
    public Helper(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException {
        this.conf = conf;
        this.hBaseAdmin = new HBaseAdmin(conf);
    }

    /**
     * Create a table if it doesn't already exist.
     * @param tableName name of table to be created
     * @param colFams column families to be set
     */
    public void createTable(byte[] tableName, byte[]... colFams) throws IOException {
        if (tableExists(tableName)) {
            // TODO : Log the event
            System.out.println("Table exists !!");
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (byte[] colFam : colFams) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFam);
            tableDescriptor.addFamily(columnDescriptor);
        }
        hBaseAdmin.createTable(tableDescriptor);
    }

    /**
     * Check if  <code>tableName</code> exists
     * @param tableName
     * @return true if the table exists
     * @throws IOException
     */
    public boolean tableExists(byte[] tableName) throws IOException {
        return hBaseAdmin.tableExists(tableName);
    }

    /**
     * Disable the table
     * @param tableName table to be disabled.
     */
    public void disableTable(byte[] tableName) throws IOException {
        if (tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
        }
    }

    /**
     * Drop the table
     * @param tableName the table to be dropped
     */
    public void dropTable(byte[] tableName) throws IOException {
        if (tableExists(tableName)) {
            disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Helper helper = new Helper(conf);
        helper.createTable(Bytes.toBytes("testTable"), // table name
                Bytes.toBytes("cf1"),                  // column family
                Bytes.toBytes("cf2"));                 // column family
        helper.disableTable(Bytes.toBytes("xxxxx"));
        helper.dropTable(Bytes.toBytes("xxxxx"));
        helper.dropTable(Bytes.toBytes("testTable"));
    }
}
