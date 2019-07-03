import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.net.URI;

/*
 * @class name:Test
 * @author:Wu Gang
 * @create: 2019-07-02 13:12
 * @description:
 */
public class Test {
    public static void main(String[] argv){
        URI a = URI.create("data/people_name_list.txt");
        UserDefineLibrary.insertWord("中国年","userDefine", 1000);
        System.out.println(ToAnalysis.parse("中国年是个好年"));
        ToAnalysis.parse("中国年是个好年");
    }
}
