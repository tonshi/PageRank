package org.tonshi.pagerank;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.PropertyConfigurator;

public class PageRankJob {

    public static final String HDFS = "hdfs://192.168.227.128:9000";
    //public static final String HDFS = "F:/Work/Dataguru-2016-3/Docs/09";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
        Map<String, String> path = new HashMap<String, String>();

        path.put("page", "F:/Work/Dataguru-2016-3/Docs/09/pagerank/people.csv");// 本地的数据文件
        path.put("pr", "F:/Work/Dataguru-2016-3/Docs/09/pagerank/peoplerank.csv");// 本地的数据文件
        
        path.put("input", HDFS + "/user/hadoop/hadoop_dev_09/pagerank");// HDFS的目录
        path.put("input_pr", HDFS + "/user/hadoop/hadoop_dev_09/pagerank/pr");// pr存储目
        path.put("tmp1", HDFS + "/user/hadoop/hadoop_dev_09/pagerank/tmp1");// 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/user/hadooop/hadoop_dev_09/pagerank/tmp2");// 临时目录,计算到得PR,覆盖input_pr

        path.put("result", HDFS + "/user/hadooop/hadoop_dev_09/pagerank/result");// 计算结果的PR
/*
        path.put("page", "F:/Work/Dataguru-2016-3/Docs/09/pagerank/people.csv");// 本地的数据文件
        path.put("pr", "F:/Work/Dataguru-2016-3/Docs/09/pagerank/peoplerank.csv");// 本地的数据文件
        
        path.put("input", HDFS + "/pagerank");// HDFS的目录
        path.put("input_pr", HDFS + "/pagerank/pr");// pr存储目
        path.put("tmp1", HDFS + "/pagerank/tmp1");// 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/pagerank/tmp2");// 临时目录,计算到得PR,覆盖input_pr

        path.put("result", HDFS + "/user/hdfs/pagerank/result");// 计算结果的PR
*/	
	    PropertyConfigurator.configure("log4j.properties");
	    System.out.println("\n main start.");
	    
        try {
/*
            AdjacencyMatrix.run(path);
            int iter = 11;
            for (int i = 0; i < iter; i++) {// 迭代执行
                PageRank.run(path);
            }*/
            Normal.run(path);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(PageRankJob.class);
        conf.setJobName("PageRank");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    public static String scaleFloat(float f) {// 保留6位小数
        DecimalFormat df = new DecimalFormat("##0.000000");
        return df.format(f);
    }
}