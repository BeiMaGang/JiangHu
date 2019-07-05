package LPA;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/*
 * @class name:LPADriver
 * @author:Wu Gang
 * @create: 2019-07-05 14:06
 * @description:
 */
public class LPADriver {
    private int itrTimes = 50;
    private String[] paths;
    private String initLabelOutPath = "/initLabel";
    private String itrProcessOutPath = "/LPAitr";
    private String resultLabelOutPath = "/result";
    private LPADriver(String[] paths) {
        this.paths = paths;
    }

    private void run() throws InterruptedException, IOException, ClassNotFoundException {
        String[] initLabelPaths = new String[]{paths[0], paths[1] + initLabelOutPath};
        InitLabel.main(initLabelPaths);

        itrProcessOutPath = paths[1] + itrProcessOutPath;
        String[] itrPaths = new String[]{paths[1] + initLabelOutPath, itrProcessOutPath + 0};
        LPAItr.main(itrPaths);
        for(int i = 0;i < itrTimes; i++){
            itrPaths = new String[]{itrProcessOutPath + i, itrProcessOutPath + (i + 1)};
            LPAItr.main(itrPaths);
            try {
                FileUtils.deleteDirectory(new File(itrProcessOutPath + i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String[] labelRankPaths = new String[]{itrProcessOutPath + itrTimes, paths[1] + resultLabelOutPath};
        LabelRank.main(labelRankPaths);
    }
    public static void main(String[] argv) throws InterruptedException, IOException, ClassNotFoundException {
        new LPADriver(argv).run();
    }
}
