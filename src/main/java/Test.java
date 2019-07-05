import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;
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
//        UserDefineLibrary.insertWord("欧阳锋","sad persio", 1000);
        System.out.println(ToAnalysis.parse("欧阳锋是个好年"));

        Result result = DicAnalysis.parse("中国年是个好年");
        Term term = result.get(0);
        System.out.println(term);
    }
}
