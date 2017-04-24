# TeraSort

## Introduction

- Tranditional solution with DivideAndConquer:  sort partially by each map task, then use one reduce task to sort globally.
- TeraSort: split the data into R splits, R is the number of reduce tasks. And the data in split i are bigger than data in split (i + 1).
- Technique difficulty of TeraSort:
  - How to define the range of the R splits.    —>   sampling, for well balanced.
  - How to quickly determine the splits of each data?    —>    trie tree, for 'quickly'.

## Algorithm flow

1. Sampling.   —>   do this in JobClient     —>   sampling some data from input data, and split them into R splits, then find the range of each split      —>    cache those split points.

   - Optional sampling ways are: 

     | ClassName            | Function                               | Constructor                       | Efficiency | Feature                 |
     | -------------------- | -------------------------------------- | --------------------------------- | ---------- | ----------------------- |
     | SplitSampler<K,V>    | Sampling first n records.              | numSamples, maxSplitsSampled      | Highest    |                         |
     | RandomSampler<K,V>   | Iterate all data, and sample randomly. | freq, numSamples, maxSplitSampled | Lowest     |                         |
     | IntervalSampler<K,V> | sampling with fixed interval.          | freq, maxSplitSampled             | Middle     | Useful for sorted data. |

   - Demo code:

   ```java
   InputSampler.Sampler<Text, NullWritable> sampler = new InputSampler.IntervalSampler<Text, NullWritable>(0.3);
           InputSampler.writePartitionFile(job, sampler);
   ```

2. Mapper.    —> simply write the \<key, value>

3. Partition.    —>  use TotalOrderPartitioner

   ```java
   job.setPartitionerClass(TotalOrderPartitioner.class);
           String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
           URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
           job.addCacheArchive(partitionUri);
   ```

4. Reducer     —> simply write the \<key, value>

## FYI

* [trie tree](http://dongxicheng.org/structure/trietree/)
* [Sampler In Hadoop](http://blog.csdn.net/andyelvis/article/details/7294811)
* [Partitoner](http://www.cnblogs.com/wuyudong/p/hadoop-partitioner.html)