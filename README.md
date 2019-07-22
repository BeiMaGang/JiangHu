# JiangHu
金庸的江湖

## 编译

- IDEA中maven 的 package 即可在target目录下生成 对应的class和对应的jar包

## jar包执行

将**JiangHu-1.0-SNAPSHOT.jar**和**JiangHu-1.0-SNAPSHOT-jar-with-dependencies.jar**与下列脚本放到同一目录下

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

## 项目结构说明

- src/ 为源代码书写处
- JiangHu/ 是脚本存放处
- report/ 报告latex书写
- graph1.pdf 和 graph2.pdf 为 Gephi生成的具体分类图
- graph.gephi 为 项目gephi具体文件，里面有详细的数据及社区聚类。
- report.pdf 为实验报告