package com.wttttt.hadoop;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-25
 * Time: 11:32
 */
public class MapSideJoin {
    public static class JoinMap extends Mapper<LongWritable, Text, Text, Text>{
        private int keyIndTable1;
        private int keyIndTable2;
        private Map<String, String> cachedTable = new HashMap<String, String>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyIndTable1 = context.getConfiguration().getInt("keyIndTable1", 0);
            keyIndTable2 = context.getConfiguration().getInt("keyIndTable2", 0);

            // create hashMap for small table
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){
                URI cachedFileUri = context.getCacheFiles()[0];
                BufferedReader br = new BufferedReader(new FileReader((new Path(cachedFileUri.toString())).toString()));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] columns = line.split("\t");
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < columns.length; i ++){
                        if (i == keyIndTable2) continue;
                        sb.append(columns[i]);
                        sb.append("\t");
                    }
                    cachedTable.put(columns[keyIndTable2], sb.toString().trim());
                }
                br.close();
            } else{
                System.err.print("No cached file exist");
                System.exit(-1);
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            String keyCol = line[keyIndTable1];
            if (cachedTable.containsKey(keyCol)) {
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < line.length; i++){
                    if (i == keyIndTable1) continue;
                    sb.append(line[i]);
                    sb.append("\t");
                }
                context.write(new Text(keyCol), new Text(cachedTable.get(keyCol) + "\t" + sb.toString().trim()));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length < 5) {
            System.err.println("Usage: com.wttttt.hadoop.OnePageTran <BiggerTable> <SmallerTable> <out> <keyIndTable1> <keyIndTable2>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        int keyInd1 = Integer.parseInt(args[3]);
        Preconditions.checkArgument(keyInd1 >= 0);
        int keyInd2 = Integer.parseInt(args[4]);
        Preconditions.checkArgument(keyInd2 >= 0);

        conf.setInt("keyIndTable1", keyInd1);
        // And the smaller table is primary on the join key
        conf.setInt("keyIndTable2", keyInd2); // smaller table

        Job job = Job.getInstance(conf, "MapSideJoin");
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(JoinMap.class);
        // no reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        String cacheFile = args[1];
        //cachedFile
        job.addCacheFile(new Path(cacheFile).toUri());

        job.waitForCompletion(true);
    }

}
