package LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/*
 * @class name:LPAItr
 * @author:Wu Gang
 * @create: 2019-07-04 14:39
 * @description:
 */
public class LPAItr {
    public static class LPAItrMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String valueName = key.toString().split(",")[0];
            String label = key.toString().split(",")[1];
            String[] values = value.toString().split("\\|");
            for(String str: values){
                String realKey = str.split(",")[0];
                String realValue = valueName + "," + str.split(",")[1] + "," + label;
                context.write(new Text(realKey), new Text(realValue));
            }
        }
    }

    public static class LPAItrReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Integer> map = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            map.clear();
            StringBuilder builder = new StringBuilder();
            for(Text text: values){
                String name = text.toString().split(",")[0];
                String label = text.toString().split(",")[2];
                int n = Integer.parseInt(text.toString().split(",")[1]);
                if(map.containsKey(label)){
                    map.put(label, map.get(label) + n);
                }else {
                    map.put(label, n);
                }
                builder.append(name).append(",").append(n).append("|");
            }
            List<Map.Entry<String,Integer>> list = new ArrayList(map.entrySet());
            list.sort((o1, o2) -> (o1.getValue() - o2.getValue()));
            String label = list.get(list.size() - 1).getKey();
            context.write(new Text(key.toString() + "," + label), new Text(builder.toString()));
        }
    }

    public static void main(String[] argv) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LPAItr");
        job.setJarByClass(LPAItr.class);
        job.setMapperClass(LPAItrMapper.class);
        job.setReducerClass(LPAItrReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argv[0]));
        FileOutputFormat.setOutputPath(job, new Path(argv[1]));
        job.waitForCompletion(true);
    }
}
