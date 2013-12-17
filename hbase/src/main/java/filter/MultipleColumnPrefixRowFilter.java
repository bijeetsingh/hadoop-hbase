package filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * This filter is used for selecting the entire row if it has qualifiers
 * which have all the supplied prefixes.
 *
 * For example if prefixes provided are 'an', 'hel' and 'run', it will
 * select the complete row if it has qualifiers with prefix 'an' AND
 * 'hel' AND 'run'.
 */
public class MultipleColumnPrefixRowFilter extends FilterBase {
    protected TreeSet<byte []> sortedPrefixes = createTreeSet();
    protected TreeSet<byte []> sortedPrefixesForRow = createTreeSet();
    private final static int MAX_LOG_PREFIXES = 5;
    private boolean filterRow = false;

    public MultipleColumnPrefixRowFilter() {
        super();
    }

    public MultipleColumnPrefixRowFilter(final byte[][] prefixes) {
        if (prefixes != null) {
            for (int i = 0; i < prefixes.length; i++) {
                if (!sortedPrefixes.add(prefixes[i]))
                    throw new IllegalArgumentException ("prefixes must be distinct");
            }
        }
    }

    public byte [][] getPrefix() {
        int count = 0;
        byte [][] temp = new byte [sortedPrefixes.size()][];
        for (byte [] prefixes : sortedPrefixes) {
            temp [count++] = prefixes;
        }
        return temp;
    }

    @Override
    public void reset() {
        this.filterRow = false;
        byte[][] tmpPrefixes = getPrefix();
        if (tmpPrefixes != null) {
            for (int i = 0; i < tmpPrefixes.length; i++) {
                sortedPrefixesForRow.add(tmpPrefixes[i]);
            }
        }
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        if (sortedPrefixesForRow.size() == 0 || kv.getBuffer() == null) {
            return ReturnCode.INCLUDE;
        } else {
            byte[] qualifier = kv.getQualifier();

            // Current qualifier is less than minimum of all
            // the prefixes, it should be included.
            if (Bytes.compareTo(qualifier, sortedPrefixesForRow.first()) < 0) {
                return ReturnCode.INCLUDE;
            }

            // As we traverse through the sorted KeyValues of a row, at no
            // point should the current qualifier be greater than the smallest
            // of prefixes provided.
            if (Bytes.startsWith(qualifier, sortedPrefixesForRow.first())) {
                sortedPrefixesForRow.pollFirst();
                return ReturnCode.INCLUDE;
            } else {
                filterRow = true;
                return ReturnCode.NEXT_ROW;
            }
        }
    }

    @Override
    public boolean filterRow() {
        // Some of the prefixes are greater than the maximum qualifier
        // in this row, which obviously can't be matched.
        if (sortedPrefixesForRow.size() != 0) {
            return true;
        }
        return filterRow;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(sortedPrefixes.size());
        for (byte [] element : sortedPrefixes) {
            Bytes.writeByteArray(out, element);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int x = in.readInt();
        this.sortedPrefixes = createTreeSet();
        for (int j = 0; j < x; j++) {
            sortedPrefixes.add(Bytes.readByteArray(in));
        }
    }

    public TreeSet<byte []> createTreeSet() {
        return new TreeSet<byte []>(new Comparator<Object>() {
            @Override
            public int compare (Object o1, Object o2) {
                if (o1 == null || o2 == null)
                    throw new IllegalArgumentException ("prefixes can't be null");

                byte [] b1 = (byte []) o1;
                byte [] b2 = (byte []) o2;
                return Bytes.compareTo (b1, 0, b1.length, b2, 0, b2.length);
            }
        });
    }

    @Override
    public String toString() {
        return toString(MAX_LOG_PREFIXES);
    }

    protected String toString(int maxPrefixes) {
        StringBuilder prefixes = new StringBuilder();

        int count = 0;
        for (byte[] ba : this.sortedPrefixes) {
            if (count >= maxPrefixes) {
                break;
            }
            ++count;
            prefixes.append(Bytes.toStringBinary(ba));
            if (count < this.sortedPrefixes.size() && count < maxPrefixes) {
                prefixes.append(", ");
            }
        }

        return String.format("%s (%d/%d): [%s]", this.getClass().getSimpleName(),
                count, this.sortedPrefixes.size(), prefixes.toString());
    }
}
