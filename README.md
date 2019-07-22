# JiangHu
金庸的江湖

## 编译

- IDEA中maven 的 package 即可在target目录下生成 对应的class和对应的jar包

## jar包执行

将JiangHu-1.0-SNAPSHOT.jar和JiangHu-1.0-SNAPSHOT-jar-with-dependencies.jar与下列脚本放到同一目录下

```bash
sh task1.sh #运行任务一
sh task2.sh task1_out #运行任务二 其中task1_out为任务一结果输出目录
sh task3.sh #运行任务三
sh PR.sh 5 #运行任务四 其中5为迭代的次数
sh LPA.sh 5 #运行任务五 其中5为迭代的次数
sh clear_hdfs.sh #清除所有任务的输出文件
sh getGraph.sh #将hdfs上的任务三和五的结果放到本地
sh format.sh #将本地的结果标准化成Gephi所需要的标准格式
```

