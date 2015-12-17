package filesystem;

import job.JobConfiguration;
import start.MainRun;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created by Fuga on 15/12/9.
 */
public class FSOperation {

    JobConf conf;
    JobConfiguration jconf;

    public FSOperation() {
        this.conf = JobConfiguration.getMainRunjobconf();
    }

    public FSOperation(JobConfiguration jconf) {
        this();
        this.jconf = jconf;
    }

    public boolean deleteFile(String path, String filename) throws IOException {
        boolean succeed = false;
        Path fspath = new Path(path+"/"+filename);
        FileSystem fs = fspath.getFileSystem(conf);
        if (fs.delete(fspath, false)) {
//            System.out.println("成功删除文件: "+fspath.getName());
            succeed = true;
        }
        else {
            System.out.println("删除文件失败: "+fspath.getName());
        }
        return succeed;
    }

    public boolean deleteDir(String path) throws IOException {
        boolean succeed = false;
        Path fspath = new Path(path);
        FileSystem fs = fspath.getFileSystem(conf);
        if (!fs.exists(fspath)) {
            succeed = true;
            return succeed;
        }
        if (fs.delete(fspath, true)) {
//            System.out.println("删除目录：<"+fspath.getName()+">成功");
            succeed = true;
        }
        else {
            System.out.println("删除目录：<" + fspath.getName() + ">失败");
        }
        return succeed;
    }

    /**
     * Move the file from FileSystem to FileSystem, overwrite the target File.
     * @param oripath
     * @param targetpath
     * @author Fuga
     * @throws IOException
     */
    public void moveFile(String oripath, String targetpath) throws IOException {
        Path fspath = new Path(oripath);
        Path tarpath = new Path(targetpath);
        FileSystem fs = fspath.getFileSystem(conf);
        FileUtil.copy(fs, fspath, fs, tarpath, true, true, conf);
        return ;
    }

    /**
     * Copy the file from FileSystem to FileSystem, overwrite the target File
     * @param oripath
     * @param targetpath
     * @throws IOException
     */
    public void copyFile(String oripath, String targetpath) throws IOException {
        Path fspath = new Path(oripath);
        Path tarpath = new Path(targetpath);
        FileSystem fs = fspath.getFileSystem(conf);
        FileUtil.copy(fs, fspath, fs, tarpath, false, true, conf);
        return ;
    }
}
