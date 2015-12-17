package job;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.log4j.PropertyConfigurator;
import start.MainRun;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Fuga on 15/12/7.
 */
public class JobConfiguration {

    static String HDFS; // filesystem address
    static String input1; // jobconfs of matrix 1 in filesystem
    static String input2; // jobconfs of matrix 2 in filesystem
    static String output; // jobconfs of output in filesystem
    static String outputname; // output result's name
    static String minSplitSize; // minimum hadoop split size
    static String maxSplitSize; // maximum hadoop split size
    static JobConf conf;

    public JobConfiguration(Map<String, String> confs) {
        this.setHDFS(confs.get("HDFS"));
        this.setInput1(confs.get("input1"));
        this.setInput2(confs.get("input2"));
        this.setOutput(confs.get("output"));
        PropertyConfigurator.configure("log4j.properties");
    }

    public static JobConf getMainRunjobconf() {
        if (conf==null) {
            conf = new JobConf(MainRun.class);
        }
        return conf;
    }

    public JobConf config(String jobname) throws IOException {
        JobConf conf = JobConfiguration.getMainRunjobconf();

        conf.setJobName(jobname);
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/core-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/hdfs-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/mapred-site.xml");
        return conf;
    }

    public JobConf config() throws IOException {
        JobConf conf = JobConfiguration.getMainRunjobconf();
////        MatrixInfo minfo = new MatrixInfo();
////        conf.set("rownum", String.valueOf(minfo.getMatrixArownum(jobconfs.get("input1"))));
////        conf.set("colnum", String.valueOf(minfo.getMatrixBcolnum(jobconfs.get("input2"))));
//        Path output_path = new Path(HDFS+"/user/tmp/output");
//        FileSystem fs = output_path.getFileSystem(conf);
//        if (fs.delete(output_path, true)) {
//            System.out.println("删除上次output目录成功");
//        }
//        else {
//            System.out.println("删除上次output目录失败");
//            System.exit(1);
//        }

        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/core-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/hdfs-site.xml");
        conf.addResource("/Users/Fuga/hadoop/hadoop-1.2.1/conf/mapred-site.xml");
        return conf;
    }

    public static String getHDFS() {
        return HDFS;
    }

    public void setHDFS(String HDFS) {
        this.HDFS = HDFS;
    }

    public String getInput1() {
        return input1;
    }

    public void setInput1(String input1) {
        this.input1 = this.getHDFS()+input1;
    }

    public String getInput2() {
        return input2;
    }

    public void setInput2(String input2) {
        this.input2 = this.getHDFS()+input2;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = this.getHDFS()+output;
    }

    public String getOutputname() {
        return outputname;
    }

    public void setOutputname(String outputname) {
        this.outputname = outputname;
    }

    public String getMinSplitSize() {
        return minSplitSize;
    }

    public void setMinSplitSize(String minSplitSize) {
        this.minSplitSize = minSplitSize;
    }

    public String getMaxSplitSize() {
        return maxSplitSize;
    }

    public void setMaxSplitSize(String maxSplitSize) {
        this.maxSplitSize = maxSplitSize;
    }
}
