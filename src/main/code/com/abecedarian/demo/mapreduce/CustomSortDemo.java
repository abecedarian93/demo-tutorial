package com.abecedarian.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by abecedarian on 2019/2/20
 * <p>
 * MapReduce 二次排序demo(其中涉及自定义key，自定义分区函数，自定义分组函数）
 * <p>
 * 在公司yarn集群运行该程序的脚本:
 * input=/user/wirelessdev/result/demo/mr/input/input1.txt
 * output=/user/wirelessdev/result/demo/mr/output/customSort
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
 * sh ${runDir}/runHadoopCommand.sh com.qunar.search.test.UserinfoPlus${mrParams} ${input} ${output} ${reduceNum}
 */

public class CustomSortDemo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new CustomSortDemo(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("./run <input> <output> <numReduceTasks>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        int numReduceTasks = Integer.parseInt(args[2]);

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CustomSortDemo.class);

        job.setMapperClass(CustomSortDemoMapper.class);
        job.setMapOutputKeyClass(CustomPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CustomSortDemoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CustomPartitioner.class); //自定义分区函数
        job.setGroupingComparatorClass(CustomGroupingComparator.class); //自定义分组函数

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(numReduceTasks);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private static class CustomSortDemoMapper extends Mapper<LongWritable, Text, CustomPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",");
            String time = items[0];
            String channelId = items[3];
            /**相同channelId分发到一个reduce，根据channelId+time进行排序**/
            context.write(new CustomPair(channelId, time), new Text(line));
        }

    }

    private static class CustomSortDemoReducer extends Reducer<CustomPair, Text, Text, NullWritable> {
        @Override
        protected void reduce(CustomPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**实现channelId排序的情况下，time进行排序 等同与sql的order by channelId,time功能**/
            for (Text value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }

    public static class CustomPair implements WritableComparable<CustomPair> {
        private Text first;
        private Text second;
        public void set(Text first, Text second) {
            this.first = first;
            this.second = second;
        }

        public CustomPair(String first, String second) {
            set(new Text(first), new Text(second));
        }

        public CustomPair(Text first, Text second) {
            set(first, second);
        }

        public CustomPair() {
            set(new Text(), new Text());
        }

        public Text getFirst() {
            return first;
        }

        public Text getSecond() {
            return second;
        }

        @Override
        public int compareTo(CustomPair o) {
            int cmp = first.compareTo(o.first);
            if (cmp != 0) {
                return cmp;
            }

            return second.compareTo(o.second);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CustomPair) {
                CustomPair wt = (CustomPair) obj;
                return first.equals(wt.first) && second.equals(wt.second);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first.readFields(in);
            second.readFields(in);
        }
    }

    public static class CustomPartitioner extends Partitioner<CustomPair, Text> {
        @Override
        public int getPartition(CustomPair key, Text value, int numReduceTasks) {

            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class CustomGroupingComparator extends WritableComparator {
        protected CustomGroupingComparator() {
            super(CustomPair.class, true);
        }
        @Override
        /** 对比两个WritableComparator**/
        public int compare(WritableComparable w1, WritableComparable w2) {
            CustomPair ip1 = (CustomPair) w1;
            CustomPair ip2 = (CustomPair) w2;
            Text l = ip1.getFirst();
            Text r = ip2.getFirst();
            return l.equals(r) ? 0 : (l.compareTo(r) > 0 ? -1 : 1);
        }
    }

}

