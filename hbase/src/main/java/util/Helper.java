package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

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

    /**
     * Populate the table with dummy time series data in the following form :
     * Row key : "random 3 digit id" + "_" + "yyyyMMdd"
     * Col family : cf1
     * Col qualifier : col + "_" + "random padded number b/w 0 and numCols"
     * Value : the suffix of col qualifier
     *
     * @param tableName Name of table to be populated
     * @param numRows number of rows to be added
     * @param numCols number of KVs in each row.
     */
    public void populateTable(byte[] tableName, byte[] colFam, int numRows, int numCols)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        for (int row = 0; row < numRows; row++) {
            Put put = new Put(genRowKey());
            for (int col =0; col < numCols; col++) {
                String colSuffix = genRandomPaddedNum(0, numCols, Integer.toString(numCols).length());
                put.add(colFam,                                 // column family
                        Bytes.toBytes("col_" + colSuffix),      // column qualifier
                        Bytes.toBytes(colSuffix));              // value
                // TODO : Use ListPut
            }
            table.put(put);
        }
        table.close();
    }

    // Generate the row key as per populateTable
    private byte[] genRowKey() {
        return Bytes.toBytes(
                genRandomPaddedNum(1, 999, 3) +                                 // a random 3 digit prefix
                "_" +                                                           // rowKey delimiter
                // TODO : Get rid of this hard coding.
                genRandDate("2012-01-01 00:00:00", "2013-12-31 23:59:59"));     // We're generating dummy data, so
                                                                                // hard coding is fine for now.
    }

    // Generate the prefix of rowkey for populateTable
    private String genRandomPaddedNum(int max, int min, int padLength) {
        int range = max - min + 1;
        return pad((int)(Math.random()*range) + min, padLength);
    }

    // prefix the number with 0s
    private String pad(int num, int padLength) {
        String paddedNum = Integer.toString(num);
        while (paddedNum.length() < padLength) {
            paddedNum = "0" + paddedNum;
        }
        return paddedNum;
    }

    // Generate random date b/w given dates
    private String genRandDate(String startDate, String endDate) {
        long offset = Timestamp.valueOf(startDate).getTime();
        long end = Timestamp.valueOf(endDate).getTime();
        long diff = end - offset + 1;
        Timestamp randTimestamp = new Timestamp(offset + (long)(Math.random() * diff));
        return new SimpleDateFormat("yyyyMMdd").format(randTimestamp);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Helper helper = new Helper(conf);
        helper.dropTable(Bytes.toBytes("testTable"));
        helper.createTable(Bytes.toBytes("testTable"), // table name
                Bytes.toBytes("cf1"),                  // column family
                Bytes.toBytes("cf2"));                 // column family

        helper.populateTable(Bytes.toBytes("testTable"), // table name
                Bytes.toBytes("cf1"),                    // column family
                500,                                       // number of rows
                100);                                     // number of columns
    }
}
