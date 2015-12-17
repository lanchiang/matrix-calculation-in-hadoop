package matrix.calculation.basic;

import custom.CustomFileOutputFormat;
import custom.CustomSortedMapWritable;
import filesystem.FSOperation;
import job.JobConfiguration;
import job.ParameterLoad;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Set;

/**
 * Created by Fuga on 15/12/1.
 */
public class MatrixMultiply {

    public static class row_col_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag; // m1 or m2

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit)context.getInputSplit();
            flag = split.getPath().getName();
//            System.out.println(flag);

            String[] tokens = ParameterLoad.DELIMITER.split(value.toString());
            if (context.getConfiguration().get("input1").contains(flag)) {
                Text v = new Text("A:" + tokens[0] + "," + tokens[2]);
                Text k = new Text(tokens[1]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
            else if (context.getConfiguration().get("input2").contains(flag)) {
                Text v = new Text("B:" + tokens[1] + "," + tokens[2]);
                Text k = new Text(tokens[0]);
                context.write(k, v);
//                System.out.println(k.toString() + " " + v.toString());
            }
        }
    }

    public static class row_col_Reducer extends Reducer<Text, Text, CustomSortedMapWritable, CustomSortedMapWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            CustomSortedMapWritable mapwritableA = new CustomSortedMapWritable();
            CustomSortedMapWritable mapwritableB = new CustomSortedMapWritable();
            for (Text line: values) {
                String val = line.toString();
                String[] kv = ParameterLoad.DELIMITER.split(val.substring(2));
                if (val.startsWith("A:")) {
                    mapwritableA.put(new IntWritable(Integer.parseInt(kv[0])), new Text(kv[1]));// kv[1] : value, kv[0] : row_n for A and col_n for B
                }
                else if (val.startsWith("B:")) {
                    mapwritableB.put(new IntWritable(Integer.parseInt(kv[0])), new Text(kv[1]));
                }
            }
            context.write(mapwritableA, mapwritableB);
        }
    }

    public static void run(JobConfiguration jconf) throws IOException, ClassNotFoundException, InterruptedException {
//
        JobConf conf = jconf.config("MatrixMultiply");
        String input1 = jconf.getInput1();
        String input2 = jconf.getInput2();

        FSOperation fso = new FSOperation(jconf);
        if (!fso.deleteDir(jconf.getHDFS()+"/matrix/tmp/output")) {
            System.exit(1);
        }
//
        conf.set("input1", input1);
        conf.set("input2", input2);
//
        Job job = new Job(conf);
        job.setJarByClass(MatrixMultiply.class);
//
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(CustomSortedMapWritable.class);
        job.setOutputValueClass(CustomSortedMapWritable.class);
//
        job.setMapperClass(row_col_Mapper.class);
        job.setReducerClass(row_col_Reducer.class);
//
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
//
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(jconf.getHDFS() + "/matrix/tmp/output"));

        if (jconf.getMinSplitSize()!=null) {
            FileInputFormat.setMinInputSplitSize(job, Long.parseLong(jconf.getMinSplitSize()));
        }
        if (jconf.getMaxSplitSize()!=null) {
            FileInputFormat.setMaxInputSplitSize(job, 1024*Long.parseLong(jconf.getMaxSplitSize()));
        }

        job.waitForCompletion(true);

        MatrixMultiplyStepTwo.run(jconf, job.getConfiguration().get("mapred.output.dir"));
    }
}

/**
 * Created by Fuga on 15/12/1.
 */
class MatrixMultiplyStepTwo {

    public static class matrix_cal_Mapper extends Mapper<Text, Text, Text, FloatWritable> {

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            SortedMapWritable matrix_a = CustomSortedMapWritable.StringToSortedMapWritable(key);
            SortedMapWritable matrix_b = CustomSortedMapWritable.StringToSortedMapWritable(value);

            Set<WritableComparable> A_keys = matrix_a.keySet();
            Set<WritableComparable> B_keys = matrix_b.keySet();
            for (Writable A_key : A_keys) {
                Text A_value = (Text) matrix_a.get(A_key);
                for (Writable B_key : B_keys) {
                    Text B_value = (Text) matrix_b.get(B_key);
                    FloatWritable iw = new FloatWritable(Float.parseFloat(A_value.toString())*Float.parseFloat(B_value.toString()));
                    context.write(new Text(A_key+","+B_key),iw);
//                    System.out.println(A_key+","+B_key+","+iw);
                }
            }
        }
    }

    public static class matrix_cal_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float result = 0;
            for (FloatWritable val : values) {
                result += val.get();
            }
            context.write(key, new FloatWritable(result));
//            System.out.println(key+","+result);
        }
    }

    public static void run(JobConfiguration jconf, String lastjoboutputpath) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = jconf.config("MatrixMultiplyStepTwo");

        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
        conf.set("outputname", jconf.getOutputname());

        FSOperation fso = new FSOperation();
        if (!fso.deleteDir(jconf.getOutput())) {
            System.exit(1);
        }

        Path input = new Path(lastjoboutputpath+"/part-r-00000");

        Job job = new Job(conf);
        job.setJarByClass(MatrixMultiplyStepTwo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(matrix_cal_Mapper.class);
        job.setReducerClass(matrix_cal_Reducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(CustomFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        Path outputpath = new Path(jconf.getOutput());
        FileOutputFormat.setOutputPath(job, outputpath);
        if (jconf.getMinSplitSize()!=null) {
            FileInputFormat.setMinInputSplitSize(job, Long.parseLong(jconf.getMinSplitSize()));
        }
        if (jconf.getMaxSplitSize()!=null) {
            FileInputFormat.setMaxInputSplitSize(job, 1024*Long.parseLong(jconf.getMaxSplitSize()));
        }

        job.waitForCompletion(true);
    }

}
