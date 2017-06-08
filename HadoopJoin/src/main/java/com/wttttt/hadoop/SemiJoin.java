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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-25
 * Time: 15:51
 */
public class SemiJoin {
    // the share setup function for SemiMapper1 and SemiMapper2
    public static HashSet<String> commonSetup(Mapper.Context context) throws IOException{
        HashSet<String> keyofSmallTable = new HashSet<String>();
        // create hashSet for small table
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI cachedFileUri = context.getCacheFiles()[0];
            BufferedReader br = new BufferedReader(new FileReader((new Path(cachedFileUri.toString())).toString()));
            String line;
            while ((line = br.readLine()) != null) {
                keyofSmallTable.add(line);
            }
            br.close();
        } else{
            System.err.print("No cached file exist");
            System.exit(-1);
        }
        return keyofSmallTable;
    }

    public static void commonMap(LongWritable key, Text value, Mapper.Context context, int keyIndTable, HashSet<String> keyofSmallTable, String tag) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String keyCol = line[keyIndTable];
        if (keyofSmallTable.contains(keyCol)) {
            StringBuilder sb = new StringBuilder();
            sb.append(tag);
            sb.append(":");   // tag for table1
            for(int i = 0; i < line.length; i++) {
                if (i == keyIndTable) continue;
                sb.append(line[i]);
                sb.append("\t");
            }
            context.write(new Text(line[keyIndTable]), new Text(sb.toString().trim()));
        }
    }

    public static class SemiMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        HashSet<String> keyofSmallTable = new HashSet<String>();
        private int keyIndTable;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyIndTable = context.getConfiguration().getInt("keyIndTable", 0);

            keyofSmallTable = commonSetup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            commonMap(key, value, context, keyIndTable, keyofSmallTable, "1");
        }
    }

    public static class SemiMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        HashSet<String> keyofSmallTable = new HashSet<String>();
        private int keyIndTable;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyIndTable = context.getConfiguration().getInt("keyIndTable", 0);

            keyofSmallTable = commonSetup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            commonMap(key, value, context, keyIndTable, keyofSmallTable, "2");
        }
    }



    public static class SemiReducer extends Reducer<Text, Text, Text, Text> {
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
            System.err.println("Usage: com.wttttt.hadoop.SemiJoin <BiggerTable> <SmallerTable> <out> <keyBiggerTable> <keySmallerTale>");
            System.exit(2);
        }
        int keyInd1 = Integer.parseInt(args[3]);
        Preconditions.checkArgument(keyInd1 >= 0);
        int keyInd2 = Integer.parseInt(args[4]);
        Preconditions.checkArgument(keyInd2 >= 0);

        String keyFilename = args[1] + "key";
        String[] arg0 = {args[1],keyFilename, Integer.toString(keyInd2)};
        KeyofSmallTable.main(arg0);

        Configuration conf = new Configuration();
        conf.setInt("keyIndTable1", keyInd1);
        // And the smaller table is primary on the join key
        conf.setInt("keyIndTable2", keyInd2); // smaller table

        Job job = Job.getInstance(conf, "SemiJoin");
        job.setJarByClass(SemiJoin.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                SemiMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                SemiMapper2.class);
        job.setReducerClass(SemiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //cachedFile
        job.addCacheFile(new Path(keyFilename).toUri());

        job.waitForCompletion(true);
    }
}
