package job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Fuga on 15/12/7.
 */
public class ParameterLoad {

    static Map<String, String> path;
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static Map<String, String> defaultparatmer() throws IOException {
        if (path!=null) return path;

        BufferedReader br = new BufferedReader(new FileReader("config.txt"));
        String line;
        path = new HashMap<>();
        while ((line=br.readLine())!=null) {
            String[] info = line.split("\t");
            path.put(info[0], info[1]);
        }
        br.close();
        return path;
    }
}
