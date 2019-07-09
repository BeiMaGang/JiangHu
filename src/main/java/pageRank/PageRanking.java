package pageRank;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;


public class PageRanking {

    public static class PageRankMap extends
            Mapper<Object, Text, Text, IntWritable> {
        private static final String PAGE = "PAGEMAP";
        private static final IntWritable one = new IntWritable(1);
        private Text temp;
        private double leaderPR;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // 人名＋pr_value  每个人的前驱对自己的贡献值
            // leader人名#follow人名#follow人名 表示稀疏矩阵
            String names = value.toString().split("\t")[0];
            String pr = value.toString().split("\t")[1];
            String[] items = names.split("#");
            if(items.length < 2) {
                return;             // 某人没有followNames
            }
            int len = items.length - 1;

            leaderPR = Double.valueOf(pr) / len;
            for(int i = 0;i < len;i++) {
                temp = new Text(items[i+1] + "+" + leaderPR);
                context.write(new Text(temp), one);
            }
           context.write(new Text(names), one);
        }

    }

    public class NewPartitioner extends HashPartitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term;
            if(key.toString().contains("+")) {
                term = key.toString().split("\\+")[0];
            }
            else {
                term = key.toString().split("#")[0];
            }
            return super.getPartition(new Text(term),value,numReduceTasks);
        }
    }

    public static class PageRankReduce extends
            Reducer<Text, IntWritable, Text, Text> {
        private static final String PAGE = "PAGEREDUCE";
        private static final double PR_CHANCE = 0.85;
        //private static final int NUM = 130;
        // 参数后续调整

        private Text cur = new Text("");
        private double valuePR = 0;
        private String leaderName;
        private String linkList;

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

//            Log.info("key is " + key.toString());


            if (key.toString().contains("+")) {      // leaderName+pr
                leaderName = key.toString().split("\\+")[0];
            } else {                                  // leaderName#followName
                leaderName = key.toString().split("#")[0];
            }
            if (cur.toString().length() == 0) {
                cur.set(leaderName);        // 初始化 cur
            }
            if (!cur.toString().equals(leaderName)) {   // 发现人名变化，调用cleanup写入信息
//                Log.info("notequal" + cur + "  " + leaderName);
                cleanup(context);
                cur.set(leaderName);    // 更新cur的值
            }
            if (key.toString().contains("+")) {     // 更新valpr 和linklist的值
                int repeatNum = 0;
                for(IntWritable v:values) {
                    repeatNum += v.get();
                }
                for(int i = 0;i < repeatNum;i++) {     // 可能出现多次相同的key( name+pr )
                    valuePR += Double.valueOf(key.toString().split("\\+")[1]);
                }
//                Log.info("REPEAT" + Integer.toString(repeatNum));
            } else {
                linkList = key.toString();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            valuePR = (1 - PR_CHANCE) + PR_CHANCE * valuePR;  // 计算pr的值，并将link_list一同写入文件
            context.write(new Text(linkList), new Text(Double.toString(valuePR)));
            // 恢复初始值
            valuePR = 0;
        }

    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();

        // String[] otherArgs = new String[]{"input/input_2.txt","output_2"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "pageRank.PageRanking");
        job.setJarByClass(PageRanking.class);
        job.setMapperClass(PageRankMap.class);
        job.setReducerClass(PageRankReduce.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);

        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        try {
            FileUtils.deleteDirectory(new File(otherArgs[otherArgs.length - 1]));
        }catch (IOException e) {
            e.printStackTrace();
        }

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
    }
}
