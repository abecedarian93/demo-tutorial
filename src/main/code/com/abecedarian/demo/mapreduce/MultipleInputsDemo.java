package com.abecedarian.demo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by tianle.li on 2018/6/4.
 * MapReduce多目录输入处理demo
 *
 * 在公司yarn集群运行该程序的脚本:
 * input=/user/wirelessdev/result/demo/mr/input/input1.txt,/user/wirelessdev/result/demo/mr/input/input2.txt
 * output=/user/wirelessdev/result/demo/mr/output/multipleInputs
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
 * sh ${runDir}/runHadoopCommand.sh com.abecedarian.demo.mapreduce.MultipleInputsDemo${mrParams} ${input} ${output} ${reduceNum}
 */
public class MultipleInputsDemo extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new MultipleInputsDemo(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        int numReduceTasks = Integer.parseInt(args[2]);
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MultipleInputsDemo.class);

        job.setMapperClass(MultipleInputsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultipleInputsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(numReduceTasks);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private static class MultipleInputsMapper extends Mapper<Object, Text, Text, Text> {

        //申明输入文件名
        private String fInputPath = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //获取输入文件名
            fInputPath = ((FileSplit) context.getInputSplit()).getPath().toString();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (fInputPath.contains("input1")) {
                //处理文件1代码
                String[] items = line.split(",");
                if (items.length < 6) {
                    return;
                }
                String joinId = items[0] + "," + items[1] + "," + items[2] + "," + items[3];
                context.write(new Text(joinId), new Text("input1," + items[4] + "," + items[5]));
            } else if (fInputPath.contains("input2")) {
                //处理文件2代码
                String[] items = line.split(",");
                if (items.length < 6) {
                    return;
                }
                String joinId = items[0] + "," + items[1] + "," + items[2] + "," + items[3];
                context.write(new Text(joinId), new Text("input2," + items[4] + "," + items[5]));
            } else if (fInputPath.contains("input3")) {
                //处理文件3代码
                String[] items = line.split(",");
                if (items.length < 6) {
                    return;
                }
                String joinId = items[0] + "," + items[1] + "," + items[2] + "," + items[3];
                context.write(new Text(joinId), new Text("input3," + items[4] + "," + items[5]));
            }

        }

    }

    private static class MultipleInputsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String input1_1 = null, input1_2 = null, input2_1 = null, input2_2 = null, input3_1 = null, input3_2 = null;
            //遍历相同key对应的value list
            for (Text value : values) {
                String line = value.toString();
                if (line.startsWith("input1")) {
                    String[] items = line.split(",");
                    input1_1 = items[1];
                    input1_2 = items[2];
                } else if (line.startsWith("input2")) {
                    String[] items = line.split(",");
                    input2_1 = items[1];
                    input2_2 = items[2];
                } else if (line.startsWith("input3")) {
                    String[] items = line.split(",");
                    input3_1 = items[1];
                    input3_2 = items[2];
                }
            }
            //结果输出
            context.write(new Text(key), new Text(input1_1 + "," + input1_2 + "," + input2_1 + "," + input2_2 + "," + input3_1 + "," + input3_2));
        }

    }
}
