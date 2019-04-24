package com.abecedarian.demo.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by abecedarian on 2019/2/20
 * <p>
 * MapReduce wordCount(计算文本单词出现次数) 入门demo
 * <p>
 * 在公司yarn集群运行该程序的脚本:
 * input=/user/wirelessdev/demo-tutorial/mr/input/wordcount.txt
 * output=/user/wirelessdev/demo-tutorial/mr/output/wordcount
 * reduceNum=16
 * mrParams=" -Dmapred.job.queue.name=wirelessdev \
 * -Dmapreduce.job.name=BidJoinShow_by_tianle.li \
 * -Dmapreduce.reduce.memory.mb=15360 \
 * -Dmapred.child.reduce.java.opts=-Xmx15360m \
 * -Dmapreduce.reduce.java.opts=-Xmx15360m \
 * -Dmapreduce.task.timeout=6000000 \
 * -Dmapred.task.timeout=6000000 \
 * -Dmapred.job.priority=VERY_HIGH \
 * -Dmapred.compress.map.output=true \
 * -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.Lz4Codec \
 * -Dmapred.output.compress=true \
 * -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"
 * <p>
 * <p>
 * sh ${runDir}/runHadoopCommand.sh com.abecedarian.demo.mapreduce.WordCountDemo${mrParams} ${input} ${output} ${reduceNum}
 */
public class WordCountDemo extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new WordCountDemo(), args);
        System.exit(status);
    }


    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0]; //输入路径
        String outputPath = args[1]; //输出路径
        int numReduceTasks = Integer.parseInt(args[2]); //reduce个数

        Configuration conf = this.getConf(); //初始化Configuration

        Job job = Job.getInstance(conf); //根据Configuration初始化Job
        job.setJarByClass(WordCountDemo.class); //加载主入口类

        job.setMapperClass(WordCountDemoMapper.class); //加载mapper函数类
        job.setMapOutputKeyClass(Text.class); //map端输出key的数据类型
        job.setMapOutputValueClass(Text.class); //map端输出value的数据类型

        job.setReducerClass(WordCountDemoReducer.class); //加载reducer函数类
        job.setOutputKeyClass(Text.class); //reduce端输出key的数据类型
        job.setOutputValueClass(Text.class); //reduce端输出value的数据类型

        FileInputFormat.setInputPaths(job, inputPath); //设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); //设置输出路径
        job.setNumReduceTasks(numReduceTasks); //设置reduce个数
        boolean success = job.waitForCompletion(true); //获取job执行结果状态
        return success ? 0 : 1;
    }

    private static class WordCountDemoMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" |\t");
            for (String word : words) {
                if (StringUtils.isNotBlank(word)) {
                    context.write(new Text(word), new Text("1"));
                }
            }
        }
    }

    private static class WordCountDemoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new Text(String.valueOf(count)));
        }
    }
}
