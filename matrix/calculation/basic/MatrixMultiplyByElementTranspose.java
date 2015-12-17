package matrix.calculation.basic;

import custom.CustomFileOutputFormat;
import filesystem.FSOperation;
import job.JobConfiguration;
import job.ParameterLoad;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Fuga on 15/12/9.
 */
public class MatrixMultiplyByElementTranspose {

    public static class byelement_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = ParameterLoad.DELIMITER.split(value.toString());
            FloatWritable v = new FloatWritable(Float.parseFloat(tokens[2]));
            Text k = new Text(tokens[1]+","+tokens[0]);
            context.write(k, v);
//            System.out.println(k.toString()+" "+v.toString());
        }
    }

    public static class byelement_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float result = 1F;
            for (FloatWritable val : values) {
                result *= val.get();
            }
            context.write(key, new FloatWritable(result));
//            System.out.println(key.toString()+" "+ result);
        }
    }

    public static void run(JobConfiguration jconf) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = jconf.config("MatrixMultiplyByElementTranspose");

        String input1 = jconf.getInput1();
        String input2 = jconf.getInput2();

        conf.set("input1", input1);
        conf.set("input2", input2);
        conf.set("outputname", jconf.getOutputname());

        FSOperation fso = new FSOperation(jconf);
        if (!fso.deleteDir(jconf.getOutput())) {
            System.exit(1);
        }

        Job job = new Job(conf);
        job.setJarByClass(MatrixMultiplyByElementTranspose.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(byelement_Mapper.class);
        job.setReducerClass(byelement_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(jconf.getOutput()));
//        FileInputFormat.setMinInputSplitSize(job, 0);
//        FileInputFormat.setMaxInputSplitSize(job, 1024*1L);

        job.waitForCompletion(true);

    }
}
