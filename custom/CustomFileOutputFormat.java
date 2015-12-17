package custom;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Fuga on 15/12/9.
 */
public class CustomFileOutputFormat extends FileOutputFormat<Text, FloatWritable> {

    @Override
    public RecordWriter<Text, FloatWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Path outputdir = FileOutputFormat.getOutputPath(taskAttemptContext);
        String subfix = taskAttemptContext.getTaskAttemptID().getTaskID().toString();
        String outputname = taskAttemptContext.getConfiguration().get("outputname");
        Path path = new Path(outputdir.toString()+"/"+outputname);
        FSDataOutputStream fileout = path.getFileSystem(taskAttemptContext.getConfiguration()).create(path);
        return new CustomRecordWriter(fileout);
    }
}
