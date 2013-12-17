package filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixRowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Client for MultipleColumnPrefixRowFilter.
 */
public class MultipleColumnPrefixRowFilterClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testTable");

        byte [][] colPrefixes = new byte [5][];
        colPrefixes[0] = Bytes.toBytes("col_03");
        colPrefixes[1] = Bytes.toBytes("col_04");
        colPrefixes[2] = Bytes.toBytes("col_06");
        colPrefixes[3] = Bytes.toBytes("col_07");
        colPrefixes[4] = Bytes.toBytes("col_09");

        Filter filter = new MultipleColumnPrefixRowFilter(colPrefixes);
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (KeyValue kv : result.raw()) {
                System.out.println("KV: " + kv + ", Value: " +
                        Bytes.toString(kv.getValue()));

            }
        }
    }
}

