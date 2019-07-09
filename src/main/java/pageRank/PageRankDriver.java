package pageRank;

import org.apache.commons.io.FileUtils;
import org.mortbay.log.Log;

import java.io.File;
import java.io.IOException;


public class PageRankDriver {

    private static int TIMES = 3; // 迭代次数
    private static String inputPath;
    private static String outputFormatDir = "/PRcorrectFormat";
    private static String outputRankDir = "/PRranking";
    private static String outputResultDir;

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        inputPath = args[0];
        outputResultDir = args[1];
        PageRankDriver.TIMES = Integer.parseInt(args[2]);
        outputFormatDir = outputResultDir + outputFormatDir;
        outputRankDir = outputResultDir + outputRankDir;
        try {
            FileUtils.deleteDirectory(new File(args[1]));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] state_correct = {inputPath, outputFormatDir};
        Log.info("FILENAME" + outputFormatDir);
        CorrectFormat.main(state_correct);
        String srcFile = outputFormatDir;
        Log.info("FILENAME" + srcFile);
        String dstDir = outputRankDir + 0;
        for (int i = 0; i < TIMES; i++) {
            String[] state_ranking = {srcFile, dstDir};
            PageRanking.main(state_ranking);
            Log.info("FILENAME" + srcFile);
            srcFile = dstDir;
            dstDir = outputRankDir + (i + 1);
        }

        for(int i = 0;i < TIMES - 1; i++) {
            try {
                FileUtils.deleteDirectory(new File(outputRankDir + i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String[] state_result = {srcFile, outputResultDir};
        SortPageRank.main(state_result);
    }
}

