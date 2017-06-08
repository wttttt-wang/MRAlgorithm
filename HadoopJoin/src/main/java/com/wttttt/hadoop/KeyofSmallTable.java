package com.wttttt.hadoop;

import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * Author: wttttt
 * Github: https://github.com/wttttt-wang
 * Date: 2017-04-26
 * Time: 14:30
 */
public class KeyofSmallTable {
    public static void main(String[] args) throws Exception{
        if (args.length < 3){
            System.err.println("<inputFilename> <outputFilename> <keyIndex>");
            System.exit(2);
        }
        int keyIndex = Integer.parseInt(args[2]);
        Preconditions.checkArgument(keyIndex >= 0);
        BufferedReader br = new BufferedReader(new FileReader(args[0]));

        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));

        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.trim().split("\t");
            bw.write(words[keyIndex], 0, words[keyIndex].length());
            bw.newLine();   // 写入一个行分隔符
        }
        bw.flush();
        bw.close();
        br.close();
    }
}
