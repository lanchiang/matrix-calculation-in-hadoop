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

import java.io.IOException;

/**
 * Created by Fuga on 15/12/15.
 */
public class MatrixAmendment {

    public static class amendment_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
            String transpose = context.getConfiguration().get("transpose");

            String[] tokens = ParameterLoad.DELIMITER.split(value.toString());
            Text k = null;
            if (transpose.equals("false")) {
                k = new Text(tokens[0]+","+tokens[1]);
            }
            else if (transpose.equals("true")) {
                k = new Text(tokens[1]+","+tokens[0]);
            }
            if (context.getConfiguration().get("input1").contains(flag)) {
                Text v = new Text("A:"+tokens[2]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
            else if (context.getConfiguration().get("input2").contains(flag)) {
                Text v = new Text("B:"+tokens[2]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
            else if (flag.contains("x.csv")) {
                String fillwithzero = context.getConfiguration().get("fillwithzero");
                int r = Integer.parseInt(context.getConfiguration().get("row"));
                if (fillwithzero.equals("false")) {
                    if (Integer.parseInt(tokens[0])>r) {
                        return;
                    }
                }
                else if (fillwithzero.equals("true")) {
                    if (Integer.parseInt(tokens[1])>r)
                        return;
                }
                Text v = new Text("X:"+tokens[2]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
        }
    }

    public static class amendment_Reducer extends Reducer<Text, Text, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float a_result = 0.0f; // h oder w
            float b_result = 0.0f; // factor.csv
            float x_result = 0.0f; // x.csv
            String transpose = context.getConfiguration().get("transpose");

            for (Text val : values) {
                if (val.toString().startsWith("A:")) {
                    a_result = Float.parseFloat(val.toString().substring(2));
                }
                else if (val.toString().startsWith("B:")) {
                    b_result = Float.parseFloat(val.toString().substring(2));
                }
                else if (val.toString().startsWith("X:")) {
                    x_result = Float.parseFloat(val.toString().substring(2));
                }
            }
            if (a_result == Float.MAX_VALUE) {
                context.write(key, new FloatWritable(x_result));
//                System.out.println(key.toString() + " " + x_result);
            }
            else {
                context.write(key, new FloatWritable(a_result * b_result));
//                System.out.println(key.toString() + " " + (a_result*b_result));
            }
        }
    }

    public static void run(JobConfiguration jconf, boolean needTranspose, int row) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = jconf.config("MatrixAmendment");

        String input1 = jconf.getInput1();
        String input2 = jconf.getInput2();

        conf.set("input1", input1);
        conf.set("input2", input2);
        conf.set("outputname", jconf.getOutputname());
        conf.set("transpose", String.valueOf(needTranspose));
        conf.set("row", String.valueOf(row));
        conf.set("fillwithzero", "false");

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

        job.setMapperClass(amendment_Mapper.class);
        job.setReducerClass(amendment_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2),
                new Path(jconf.getHDFS()+"/matrix/decomposition/nmf/x.csv"));// 加载3个输入数据集 w.csv oder h.csv; factor.csv; x.csv
        FileOutputFormat.setOutputPath(job, new Path(jconf.getOutput()));
        if (jconf.getMinSplitSize()!=null) {
            FileInputFormat.setMinInputSplitSize(job, Long.parseLong(jconf.getMinSplitSize()));
        }
        if (jconf.getMaxSplitSize()!=null) {
            FileInputFormat.setMaxInputSplitSize(job, 1024*Long.parseLong(jconf.getMaxSplitSize()));
        }

        job.waitForCompletion(true);
    }

    public static void run(JobConfiguration jconf, boolean needTranspose, int row, boolean fillwithzero) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = jconf.config("MatrixAmendment");

        String input1 = jconf.getInput1();
        String input2 = jconf.getInput2();

        conf.set("input1", input1);
        conf.set("input2", input2);
        conf.set("outputname", jconf.getOutputname());
        conf.set("transpose", String.valueOf(needTranspose));
        conf.set("row", String.valueOf(row));
        conf.set("fillwithzero", String.valueOf(fillwithzero));

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

        job.setMapperClass(amendment_Mapper.class);
        job.setReducerClass(amendment_Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2),
                new Path(jconf.getHDFS()+"/matrix/decomposition/nmf/x.csv"));// 加载3个输入数据集 w.csv oder h.csv; factor.csv; x.csv
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
