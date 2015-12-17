package matrix.meta;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

import Jama.Matrix;
import job.JobConfiguration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author Fuga
 */

public class MatrixUtils {
	Map<Integer, Map<Integer, Float>> matrixval;
	static int n;
	static int m;

	public void readFile(String filepath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		String line;
		matrixval = new TreeMap<>();
		int max_col = 0;
		while ((line=br.readLine())!=null) {
			String[] info = line.split(",");
			if (matrixval.containsKey(Integer.parseInt(info[0])-1)) {
				matrixval.get(Integer.parseInt(info[0])-1).put(Integer.parseInt(info[1])-1, Float.parseFloat(info[2]));
			}
			else {
				Map<Integer, Float> map = new TreeMap<>();
				map.put(Integer.parseInt(info[1])-1, Float.parseFloat(info[2]));
				matrixval.put(Integer.parseInt(info[0])-1, map);
			}
			if (max_col<Integer.parseInt(info[1])) max_col = Integer.parseInt(info[1]);
		}
		n = matrixval.size();
		m = max_col;
	}
	
	public Matrix getX() {
		Matrix X = new Matrix(n, m);
		
		Iterator<Entry<Integer, Map<Integer, Float>>> iter = matrixval.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<Integer, Map<Integer, Float>> pairs = iter.next();
			Map<Integer, Float> colvalue = pairs.getValue();

			Iterator<Entry<Integer, Float>> inner_iter = colvalue.entrySet().iterator();
//			Float sum = 0;
//			while (inner_iter.hasNext()) {
//				Entry<Integer, Float> e = inner_iter.next();
//				sum += e.getValue();
//			}
//			inner_iter = colvalue.entrySet().iterator();
			while (inner_iter.hasNext()) {
				Entry<Integer, Float> e = inner_iter.next();
//				X.set(pairs.getKey(), e.getKey(), e.getValue()/sum);
				System.out.println(pairs.getKey()+"\t"+e.getKey()+"\t"+e.getValue());
				X.set(pairs.getKey(), e.getKey(), e.getValue());
			}
		}

		return X;
	}

	public static void print(Matrix matrix) {
		int n = matrix.getRowDimension();
		int m = matrix.getColumnDimension();
		for (int i = 0; i<n; i++) {
			for (int j = 0; j<m; j++) {
				System.out.print(matrix.get(i,j)+" ");
			}
			System.out.println();
		}
		System.out.println();
	}

	public void matrixToDisc(Matrix matrix, String path) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(path));
			for (int i = 0;i<matrix.getRowDimension();i++) {
				for (int j = 0;j<matrix.getColumnDimension();j++) {
					int _i = i+1;
					int _j = j+1;
					bw.write(_i + "," + _j + "," + matrix.get(i, j));
					bw.newLine();
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void matrixToFileSystem(Matrix matrix, String fspath) {
		try {
//			FileSystem fs = FileSystem.get(JobConfiguration.getMainRunjobconf());
			FileSystem fs = FileSystem.get(URI.create(JobConfiguration.getHDFS()), JobConfiguration.getMainRunjobconf());
//			FSDataOutputStream outputStream = fs.create(new Path(JobConfiguration.getHDFS()+fspath), true);
			FSDataOutputStream outputStream = fs.create(new Path(fspath), true);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
			for (int i = 0;i<matrix.getRowDimension();i++) {
				for (int j = 0;j<matrix.getColumnDimension();j++) {
					int _i = i+1;
					int _j = j+1;
					bw.write(_i + "," + _j + "," + matrix.get(i, j));
					bw.newLine();
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
