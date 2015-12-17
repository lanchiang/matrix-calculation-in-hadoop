package custom;

import org.apache.hadoop.io.*;

import java.util.Set;

/**
 * Created by Fuga on 15/12/1.
 */
public class CustomSortedMapWritable extends SortedMapWritable {
    @Override
    public String toString() {
        String s = new String("");
        Set<WritableComparable> keys = this.keySet();
        for (Writable key : keys) {
            Text value = (Text) this.get(key);
            s = s + key.toString() + "=" + value.toString() + ",";
        }
        return s;
    }

    public static SortedMapWritable StringToSortedMapWritable(Text text) {
        SortedMapWritable smw = new SortedMapWritable();
        String str = text.toString();
        String[] arr = str.split(",");
        for (String s : arr) {
            String[] info = s.split("=");
            smw.put(new IntWritable(Integer.parseInt(info[0])), new Text(info[1]));
        }
        return smw;
    }
}
