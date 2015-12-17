package matrix.calculation.metrics;

import filesystem.FSOperation;
import job.JobConfiguration;
import job.ParameterLoad;
import matrix.meta.MatrixInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Fuga on 15/12/11.
 */
public class MatrixDistance {

    JobConfiguration jconf;

    public MatrixDistance() {
        try {
            jconf = new JobConfiguration(ParameterLoad.defaultparatmer());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public MatrixDistance(JobConfiguration jconf) {
        this.jconf = jconf;
    }

    public float euclideanDistanceSquare(String uriA, String uriB) throws InterruptedException, IOException, ClassNotFoundException {
        float distance = 0F;
        int n = MatrixInfo.matrixRowcount(jconf.getHDFS()+uriA);
        int m = MatrixInfo.matrixColcount(jconf.getHDFS()+uriA);

        if (n != MatrixInfo.matrixRowcount(jconf.getHDFS()+uriB) || m != MatrixInfo.matrixColcount(jconf.getHDFS()+uriB)) {
            throw new IllegalArgumentException("Matrix dimensions must agree");
        }

        jconf.setInput1(uriA);
        jconf.setInput2(uriB);
        jconf.setOutput("/matrix/distance");
        return MatrixDistance_Hadoop.run(jconf, "euclidean");
    }
}

class MatrixDistance_Hadoop {

    public static class matrixDistanceMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = ParameterLoad.DELIMITER.split(value.toString());
            Text k = new Text(tokens[0]+","+tokens[1]);
            FloatWritable v = new FloatWritable(Float.parseFloat(tokens[2]));
            context.write(k, v);
//            System.out.println(k.toString()+"\t"+v.toString());
        }
    }

    public static class euclideanDistanceReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private static float distance = 0.0f;
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            List<Float> list = new ArrayList<>();
            for (FloatWritable val : values) {
                list.add(val.get());
            }
            float result = (float) Math.pow(list.get(0)-list.get(1),2.0);
            distance += result;
        }

        public static float getDistance() {
            return distance;
        }

        public static void setDistance(float distance) {
            euclideanDistanceReducer.distance = distance;
        }
    }


    public static float run(JobConfiguration jconf, String metric) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = jconf.config("MatirxEuclideanDistance");
        String input1 = jconf.getInput1();
        String input2 = jconf.getInput2();
        conf.set("input1", input1);
        conf.set("input2", input2);

        FSOperation fso = new FSOperation();
        if (!fso.deleteDir(jconf.getOutput())) {
            System.exit(1);
        }

        Job job = new Job(conf);

        job.setJarByClass(MatrixDistance_Hadoop.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        switch (metric) {
            case "euclidean":
                job.setReducerClass(euclideanDistanceReducer.class);
            default:
                job.setReducerClass(euclideanDistanceReducer.class);
        }
        job.setMapperClass(matrixDistanceMapper.class);

//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(jconf.getOutput()));

        if (jconf.getMinSplitSize()!=null) {
            FileInputFormat.setMinInputSplitSize(job, Long.parseLong(jconf.getMinSplitSize()));
        }
        if (jconf.getMaxSplitSize()!=null) {
            FileInputFormat.setMaxInputSplitSize(job, 1024*Long.parseLong(jconf.getMaxSplitSize()));
        }

        job.waitForCompletion(true);
        float distance = euclideanDistanceReducer.getDistance();
        euclideanDistanceReducer.setDistance(0.0f);
        return distance;
    }
}