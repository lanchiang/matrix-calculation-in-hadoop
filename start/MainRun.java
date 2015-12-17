package start;

import job.JobConfiguration;
import job.ParameterLoad;
import matrix.calculation.decomposition.NMFDecomposition;
import matrix.calculation.metrics.MatrixDistance;
import org.apache.hadoop.io.FloatWritable;

import java.io.IOException;

/**
 * Created by Fuga on 15/12/10.
 */
public class MainRun {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        JobConfiguration jconf = new JobConfiguration(ParameterLoad.defaultparatmer());
        jconf.setInput1("/matrix/decomposition/nmf/x.csv");
        jconf.setOutput("/matrix/decomposition/nmf/output");
        NMFDecomposition nmf = new NMFDecomposition(5);
        nmf.factorizeNMF_Di(jconf);
    }
}
