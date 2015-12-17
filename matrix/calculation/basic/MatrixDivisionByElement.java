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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Fuga on 15/12/9.
 */
public class MatrixDivisionByElement {

    public static class byelement_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag; // m1 oder m2
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
//            System.out.println(flag);

            String[] tokens = ParameterLoad.DELIMITER.split(value.toString());
            if (context.getConfiguration().get("input1").contains(flag)) {
//                FloatWritable v = new FloatWritable(Float.parseFloat(tokens[2]));
                Text v = new Text("A:"+tokens[2]);
                Text k = new Text(tokens[0]+","+tokens[1]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
            else if (context.getConfiguration().get("input2").contains(flag)) {
                Text v = new Text("B:"+tokens[2]);
                Text k = new Text(tokens[0]+","+tokens[1]);
                context.write(k, v);
//                System.out.println(k.toString()+" "+v.toString());
            }
        }
    }

    public static class byelement_Reducer extends Reducer<Text, Text, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float dividend = 1F;
            float divisor = 1F;
            float result = 1F;
            for (Text val : values) {
                if (val.toString().contains("A:")) {
                    dividend = Float.parseFloat(val.toString().substring(2));
                }
                else if (val.toString().contains("B:")) {
                    divisor = Float.parseFloat(val.toString().substring(2));
                }
            }
            result = (divisor==0)?Float.MAX_VALUE:(dividend/divisor);
            context.write(key, new FloatWritable(result));
//            System.out.println(key.toString()+" "+ result);
        }
    }

    public static void run(JobConfiguration jconf) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = jconf.config("MatrixMultiplyByElement");

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
        job.setJarByClass(MatrixDivisionByElement.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(byelement_Mapper.class);
        job.setReducerClass(byelement_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(jconf.getOutput()));
        if (jconf.getMinSplitSize()!=null) {
            FileInputFormat.setMinInputSplitSize(job, Long.parseLong(jconf.getMinSplitSize()));
        }
        if (jconf.getMaxSplitSize()!=null) {
            FileInputFormat.setMaxInputSplitSize(job, 1024*Long.parseLong(jconf.getMaxSplitSize()));
        }

        job.waitForCompletion(true);

    }
}
