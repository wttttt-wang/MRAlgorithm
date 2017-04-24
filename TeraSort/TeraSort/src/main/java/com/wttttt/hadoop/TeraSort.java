package com.wttttt.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-04-24
 * Time: 10:46
 */
public class TeraSort {

    public static class SplitMapper extends Mapper<Text, Text, Text, Text>{
    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values){
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        if(args.length < 3) {
            System.err.println("Usage: com.wttttt.hadoop.TeraSort <in> <out> <sampleFreq>.");
            System.exit(2);
        }

        float sampleFreq = Float.parseFloat(args[2]);
        if (sampleFreq <= 0 || sampleFreq > 1.0){
            System.err.print("SampleFreq should be in (0, 1].");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "TeraSort Job");
        job.setJarByClass(TeraSort.class);

        // TextInputFormat is default InputFormat
        // The key of TextInputFormat is not line number, but the offset of whole file
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        // job.setNumReduceTasks(2);  //  For testing

        // KeyValueTextInputFormat extends FileInputFormat
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Sampling
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.IntervalSampler<Text, Text>(sampleFreq);
        InputSampler.writePartitionFile(job, sampler);

        // TotalOrderPartitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
        URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
        job.addCacheArchive(partitionUri);

        job.waitForCompletion(true);
    }
}
