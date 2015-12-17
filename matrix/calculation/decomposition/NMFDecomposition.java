package matrix.calculation.decomposition;

import Jama.Matrix;
import filesystem.FSOperation;
import job.JobConfiguration;
import matrix.calculation.basic.*;
import matrix.calculation.metrics.MatrixDistance;
import matrix.meta.MatrixInfo;
import matrix.meta.MatrixUtils;

import java.io.IOException;

/**
 * Created by Fuga on 15/12/10.
 */
public class NMFDecomposition {

    int r; // dimension of r
    int maxIterations = 100;
    float tolerance = 0.01F;

    int n; // row dimension of matrix one
    int m; // column dimension of matrix two
    JobConfiguration jconf;
    private static String datapath = "/matrix/decomposition/nmf";

    public NMFDecomposition(int r) {
        this.r = r;
    }

    public NMFDecomposition(int r, int maxIterations) {
        this(r);
        this.maxIterations = maxIterations;
    }

    public NMFDecomposition(int r, float tolerance) {
        this(r);
        this.tolerance = tolerance;
    }

    public NMFDecomposition(int r, int maxIterations, float tolerance) {
        this(r, maxIterations);
        this.tolerance = tolerance;
    }

    private void calOriginMatrixDimension(String uri) {
        n = MatrixInfo.matrixRowcount(uri);
        m = MatrixInfo.matrixColcount(uri);
    }

    private boolean initialWH() {
        boolean succeed = false;
        try {
            Matrix W = new Matrix(n, r);
            Matrix H = new Matrix(r, m);
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < r; j++) {
                    W.set(i, j, Math.random());
                }
            }
            for (int i = 0; i < r; i++) {
                for (int j = 0; j < m; j++) {
                    H.set(i, j, Math.random());
                }
            }
            MatrixUtils mu = new MatrixUtils();
            mu.matrixToFileSystem(W, datapath+"/w.csv");
            mu.matrixToFileSystem(W.transpose(), datapath+"/wt.csv");
            mu.matrixToFileSystem(H, datapath+"/h.csv");
            mu.matrixToFileSystem(H.transpose(), datapath+"/ht.csv");
            succeed = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return succeed;
        }
    }

    public void factorizeNMF_Di(JobConfiguration jobconf) throws IOException, InterruptedException, ClassNotFoundException {
        this.jconf = jobconf;
        calOriginMatrixDimension(jconf.getInput1());
        if (!initialWH()) return;

        int iterations = 0;
//        jconf.setOutput("/matrix/decomposition/nmf/output");
        String matrices_path = "/matrix/decomposition/nmf";
        FSOperation fso = new FSOperation(jconf);
        float distance = 0.0f;
        while (iterations < maxIterations) {
            // Calculate W(transpose)*W
            matrixMultiply("/wt.csv", "/w.csv", matrices_path, "wtw.csv", jconf);
            fso.moveFile(jconf.getOutput()+"/wtw.csv", jconf.getHDFS()+matrices_path+"/wtw.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W(T)*W");

            // Update Matrix H
            // Schritt eins, WT*X und WTW*H berechnen
            matrixMultiply("/wt.csv", "/x.csv", matrices_path, "wtx.csv", jconf);
            fso.moveFile(jconf.getOutput()+"/wtx.csv", jconf.getHDFS()+matrices_path+"/wtx.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W(T)*X");

            matrixMultiply("/wtw.csv", "/h.csv", matrices_path, "wtwh.csv", jconf);
            fso.moveFile(jconf.getOutput()+"/wtwh.csv", jconf.getHDFS()+matrices_path + "/wtwh.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W(T)W*H");

            // Schritt zwei, calculate factor
            matrixDivisionByElement("/wtx.csv", "/wtwh.csv", matrices_path, "factor.csv", jconf);
            fso.moveFile(jconf.getOutput()+"/factor.csv", jconf.getHDFS()+matrices_path + "/factor.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W(T)*X / W(T)W*H by element");

            // Schritt drei, Amend matrix, update matrix H
            matrixAmendmentForH("/h.csv", "/factor.csv", matrices_path, "h.csv", false, jconf);
            fso.moveFile(jconf.getOutput()+"/h.csv", jconf.getHDFS()+"/matrix/tmp/h.csv");
            matrixAmendmentForH("/h.csv", "/factor.csv", matrices_path, "ht.csv", true, jconf);
            fso.moveFile(jconf.getOutput()+"/ht.csv", jconf.getHDFS()+matrices_path + "/ht.csv");
            fso.moveFile(jconf.getHDFS()+"/matrix/tmp/h.csv", jconf.getHDFS()+matrices_path + "/h.csv");

            // Schritt drei, update matrix H
//            matrixMultiplyByElement("/h.csv", "/factor.csv", matrices_path, "h.csv", jconf);
            // update matrix HT
//            matrixMultiplyByElementTranspose("/h.csv", "/factor.csv", matrices_path, "ht.csv", jconf);

            // Calculate H*H(transpose)
            matrixMultiply("/h.csv", "/ht.csv", matrices_path, "hht.csv", jconf);
            fso.moveFile(jconf.getOutput()+"/hht.csv", jconf.getHDFS()+matrices_path + "/hht.csv");
            System.out.println("Iteration:"+iterations+",Calculate H*H(T)");

            // Update matrix W
            // Schritt eins, X*HT und W*HHT berechnen
            matrixMultiply("/x.csv", "/ht.csv", matrices_path, "xht.csv", jconf);
            fso.moveFile(jconf.getOutput() + "/xht.csv", jconf.getHDFS() + matrices_path + "/xht.csv");
            System.out.println("Iteration:" + iterations + ",Calculate X*H(T)");

            matrixMultiply("/w.csv", "/hht.csv", matrices_path, "whht.csv", jconf);
            fso.moveFile(jconf.getOutput() + "/whht.csv", jconf.getHDFS() + matrices_path + "/whht.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W*HH(T)");

            // Schritt zwei, calculate factor
            matrixDivisionByElement("/xht.csv", "/whht.csv", matrices_path, "factor.csv", jconf);
            fso.moveFile(jconf.getOutput() + "/factor.csv", jconf.getHDFS() + matrices_path + "/factor.csv");
            System.out.println("Iteration:" + iterations + ",Calculate X*H(T) / W*HH(T) by element");

            // Schritt drei, amend matrix, update matrix W
            matrixAmendmentForW("/w.csv", "/factor.csv", matrices_path, "w.csv", false, true, jconf);
            fso.moveFile(jconf.getOutput() + "/w.csv", jconf.getHDFS() + "/matrix/tmp/w.csv");
            matrixAmendmentForW("/w.csv", "/factor.csv", matrices_path, "wt.csv", true, true, jconf);
            fso.moveFile(jconf.getOutput()+"/wt.csv", jconf.getHDFS()+matrices_path + "/wt.csv");
            fso.moveFile(jconf.getHDFS()+"/matrix/tmp/w.csv", jconf.getHDFS()+matrices_path + "/w.csv");

            // Schritt drei, update matrix W
//            matrixMultiplyByElement("/w.csv", "/factor.csv", matrices_path, "w.csv", jconf);
            // update matrix WT
//            matrixMultiplyByElementTranspose("/w.csv", "/factor.csv", matrices_path, "wt.csv", jconf);

            // calculate W*H
            matrixMultiply("/w.csv", "/h.csv", matrices_path, "wh.csv", jconf);
            fso.moveFile(jconf.getOutput() + "/wh.csv", jconf.getHDFS() + matrices_path + "/wh.csv");
            System.out.println("Iteration:" + iterations + ",Calculate W*H by element");

            // Check for convergence
            MatrixDistance md = new MatrixDistance();
            float new_distance = md.euclideanDistanceSquare(matrices_path+"/wh.csv", matrices_path+"/x.csv");
            float distance_diff = (new_distance>distance)?(new_distance - distance):(distance-new_distance);
            System.out.println("Distance difference:" + distance_diff);
            if (distance_diff < tolerance) {
                break;
            }
            distance = new_distance;
            iterations++;
        }
    }

    float euclideanDistanceSquare(Matrix A, Matrix B) throws IllegalArgumentException {
        float distance = 0F;
        int p = A.getRowDimension();
        int q = A.getColumnDimension();

        if (p != B.getRowDimension() || q != B.getColumnDimension()) {
            throw new IllegalArgumentException("Matrix dimensions must agree");
        }

        for (int i = 0; i < p; i++) {
            for (int j = 0; j < q; j++) {
                distance += Math.pow((A.get(i, j) - B.get(i, j)), 2.0);
            }
        }

        return distance;
    }

    private void matrixMultiply(String input1, String input2, String matrices_path, String outputname, JobConfiguration jconf)
            throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path+input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixMultiply.run(jconf);
    }

    private void matrixMultiplyByElement(String input1, String input2, String matrices_path, String outputname, JobConfiguration jconf)
            throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path+input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixMultiplyByElement.run(jconf);
    }

    private void matrixMultiplyByElementTranspose(String input1, String input2, String matrices_path, String outputname, JobConfiguration jconf)
            throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path+input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixMultiplyByElementTranspose.run(jconf);
    }

    private void matrixDivisionByElement(String input1, String input2, String matrices_path, String outputname, JobConfiguration jconf)
            throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path+input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixDivisionByElement.run(jconf);
    }

    private void matrixAmendmentForH(String input1, String input2, String matrices_path, String outputname, boolean needTranspose,
                                     JobConfiguration jconf) throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path+input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixAmendment.run(jconf, needTranspose, r);
    }

    private void matrixAmendmentForW(String input1, String input2, String matrices_path, String outputname, boolean needTranspose,
                                     boolean fillwithzero, JobConfiguration jconf) throws InterruptedException, IOException, ClassNotFoundException {
        jconf.setInput1(matrices_path + input1);
        jconf.setInput2(matrices_path + input2);
        jconf.setOutputname(outputname);
        MatrixAmendment.run(jconf, needTranspose, r, fillwithzero);
    }
}

class NMFDecompositionForSymmetry {
}