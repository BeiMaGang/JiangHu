package PR;

import org.apache.commons.io.FileUtils;
import org.mortbay.log.Log;

import java.io.File;
import java.io.IOException;

/*
调用时给出输入文件夹和输出文件夹。
 */


public class PageRankDriver {

    private static int TIMES = 3; // 迭代次数
    private static String inputPath = new String("input/novels");
    private static String outputFormatDir = new String("/PRcorrectFormat");
    private static String outputRankDir = new String("/PRranking");
    private static String outputResultDir = new String("PRresult");
    private static String outputSortDir = new String("/PRsort");

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 3) {
            System.err.println("inputDir, outputDir, TIMES for Iteration \n");
            System.exit(2);
        }
        inputPath = args[0];
        outputResultDir = args[1];
        TIMES = Integer.valueOf(args[2]);
        try {
            FileUtils.deleteDirectory(new File(args[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 修改格式符合pageRank的输入
        String[] state_correct = {inputPath, outputResultDir + outputFormatDir};

        Log.info("FILENAME: " + outputFormatDir);
        CorrectFormat.main(state_correct);

        String srcFile = new String(outputResultDir + outputFormatDir);
        Log.info("FILENAME: " + srcFile);
        String dstDir = new String(outputResultDir + outputRankDir);
        for (int i = 0; i < TIMES; i++) {
            String[] state_ranking = {srcFile, dstDir};
            PageRanking.main(state_ranking);
            Log.info("FILENAME: " + srcFile);
            srcFile = dstDir;
            dstDir = outputResultDir + outputRankDir + i;
        }


        String[] state_result = {srcFile, outputResultDir + outputSortDir};
        SortPageRank.main(state_result);
    }
}

