import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

/*
 * @class name:SplitText
 * @author:Wu Gang
 * @create: 2019-07-02 14:40
 * @description:
 */
public class SplitText {
    public static class SplitMapper extends Mapper<Object, Text, Text, Text> {
        private List<String> peopleLists = new LinkedList<>();
        @Override
        protected void setup(Context context) throws IOException {
            URI[] uris = context.getCacheFiles();
            for(URI u: uris){
                BufferedReader dataReader = new BufferedReader(new FileReader(u.toString()));
                String line;
                while ((line = dataReader.readLine()) != null){
                    UserDefineLibrary.insertWord(line, "person_name", 1000);
                    peopleLists.add(line);
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            Result result = ToAnalysis.parse(content);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.substring(0,fileName.indexOf("."));
            List<String> realValue = new LinkedList<>();
            for(Term term: result.getTerms()){
                String pList = "_" + StringUtils.join("_", peopleLists) + "_";
                if(pList.contains("_" + term.getName() + "_")){
                    realValue.add(term.getName());
                }
            }
            if(realValue.size() > 0)
                context.write(new Text(fileName + "#" + key.toString()), new Text(StringUtils.join("\t", realValue)));
        }
    }

    public static class SplitReducer extends Reducer<Text, Text, Text, Text>{
        private MultipleOutputs<Text, Text> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(values.iterator().hasNext()) {
                for (Text text : values) {
//                    context.write(null, text);
                    outputs.write(text, new Text(""), key.toString().split("#")[0]);
                }
            }
        }
    }

    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
//        FileUtil.fullyDelete(new File(argv[1]));
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, argv)).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage:Merge and duplicate removal <in> <out> <people name list>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "分词");
        job.setJarByClass(SplitText.class);
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SplitReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(otherArgs[2]).toUri());
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); // 避免生成空文件
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
