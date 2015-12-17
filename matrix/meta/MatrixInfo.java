package matrix.meta;

import job.JobConfiguration;
import job.ParameterLoad;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import start.MainRun;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Created by Fuga on 15/12/7.
 */
public class MatrixInfo {

    /**
     * get row number of the matrix stored in sparse way
     * @param uri path of the matrix in filesystem
     * @return the row number
     */
    public static int matrixRowcount(String uri) {
        JobConf conf = JobConfiguration.getMainRunjobconf();
        int maxrowcount = 0;
        try {
            FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
            InputStream inputStream = filesystem.open(new Path(uri));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line=br.readLine())!=null) {
                String[] kv = ParameterLoad.DELIMITER.split(line);
                int num = Integer.parseInt(kv[0]);
                if (num>maxrowcount) maxrowcount = num;
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return maxrowcount;
        }
    }

    /**
     * get column number of the matrix stored in sparse way
     * @param uri path of the matrix in filesystem
     * @return the column number
     */
    public static int matrixColcount(String uri) {
        JobConf conf = JobConfiguration.getMainRunjobconf();
        int maxcolcount = 0;
        try {
            FileSystem filesystem = FileSystem.get(URI.create(uri), conf);
            InputStream inputStream = filesystem.open(new Path(uri));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line=br.readLine())!=null) {
                String[] kv = ParameterLoad.DELIMITER.split(line);
//                if ((kv[0]).equals("1")) {
//                    int num = Integer.parseInt(kv[1]);
//                    if (num>maxcolcount) maxcolcount = num;
//                }
                int num = Integer.parseInt(kv[1]);
                if (num>maxcolcount) maxcolcount = num;
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return maxcolcount;
        }
    }
}
