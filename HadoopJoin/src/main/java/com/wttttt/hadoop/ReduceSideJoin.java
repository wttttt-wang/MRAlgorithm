package com.wttttt.hadoop;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-25
 * Time: 10:05
 */
public class ReduceSideJoin {
    public static class Table1Mapper extends Mapper<LongWritable, Text, Text, Text>{
        private int keyIndTable1;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyIndTable1 = context.getConfiguration().getInt("KeyIndTable1", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");

            StringBuilder sb = new StringBuilder();
            sb.append("1" + ":");   // tag for table1
            for(int i = 0; i < line.length; i++) {
                if (i == keyIndTable1) continue;
                sb.append(line[i]);
                sb.append("\t");
            }
            context.write(new Text(line[keyIndTable1]), new Text(sb.toString().trim()));
        }
    }

    public static class Table2Mapper extends Mapper<LongWritable, Text, Text, Text>{

        private int keyIndTable2;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyIndTable2 = context.getConfiguration().getInt("keyIndTable2", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\t");
            StringBuilder sb = new StringBuilder();
            sb.append("2" + ":");   // tag for table2
            for (int i = 0; i < line.length; i++){
                if (i == keyIndTable2) continue;
                sb.append(line[i]);
                sb.append("\t");
            }
            context.write(new Text(line[keyIndTable2]), new Text(sb.toString().trim()));
        }
    }


    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> table1 = new ArrayList<String>();
            ArrayList<String> table2 = new ArrayList<String>();
            // val --> 1:col1\tcol2\tcol3...\tcoln
            for (Text val : values){
                String[] valSplited = val.toString().trim().split(":");
                if (valSplited.length < 2) continue;
                // Attention: u cannot use 'valSplited[0] == "1"' here!!!
                if (valSplited[0].equals("1")){
                    table1.add(valSplited[1]);
                } else{
                    table2.add(valSplited[1]);
                }
            }
            // Calculate cartesian product
            for (String line1 : table1){
                for (String line2 : table2){
                    context.write(key, new Text(line1 + "\t" + line2));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length < 5) {
            System.err.println("Usage: com.wttttt.hadoop.OnePageTran <inTable1> <inTable2> <out> <keyIndTable1> <keyIndTable2>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        int keyInd1 = Integer.parseInt(args[3]);
        Preconditions.checkArgument(keyInd1 >= 0);
        int keyInd2 = Integer.parseInt(args[4]);
        Preconditions.checkArgument(keyInd2 >= 0);

        conf.setInt("keyIndTable1", keyInd1);
        conf.setInt("keyIndTable2", keyInd2);

        Job job = Job.getInstance(conf, "ReduceSideJoin");
        job.setJarByClass(ReduceSideJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                Table1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                Table2Mapper.class);
        // no combiner available
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
