package com.wttttt.hadoop;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang/hadoop_inaction
 * Date: 2017-04-25
 * Time: 11:06
 */
public class Test {
    public static void main(String[] args) throws Exception{
        int expectedInsertions = 20;
        Funnel<CharSequence> funnel = Funnels.stringFunnel();
        // // FYI, for 3%, we always get 5 hash functions
        BloomFilter<CharSequence> bf = BloomFilter.create(funnel, expectedInsertions, 0.03);

        // create bf for small table
        BufferedReader br = new BufferedReader(new FileReader("input/table1key.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            bf.put(line);
        }
        br.close();
        for (int i = 0; i < 100; i++){
            System.out.print(bf.mightContain(new Utf8(Integer.toString(i))) + "\n");
        }
    }
}
