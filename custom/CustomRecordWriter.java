package custom;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Fuga on 15/12/9.
 */
public class CustomRecordWriter extends RecordWriter<Text, FloatWritable> {
    private PrintWriter out;
    private String seperator = "\t";

    public CustomRecordWriter(FSDataOutputStream fileout) {
        out = new PrintWriter(fileout);
    }

    @Override
    public void write(Text key, FloatWritable value) throws IOException, InterruptedException {
        out.print(key.toString()+seperator+value.toString()+"\n");
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out.close();
    }
}
