# TFIDF

## Introduction

* For calculating the weight of words.

  W = -n * log(N / T),

  W: weight of the word

  n: the occurrence number of the word in article/sentence

  N: the occurrence number of the word in corpus

  T: the occurrence number of all words in corpus

* **Tf-idf = -(nij / sum(nij) ) * Log(Dw / Dt)**

  nij: the occurrence number of the word in the article

  sum(nij): the occurrence of all words in the article

  Dw: all documents which include this word

  Dt: number of all documents

* **tf–idf**, short for **term frequency–inverse document frequency**

* For reflecting **how important a word is to a documen**t in a collection or corpus.

* 注：tf是该词在文章中的频率，但是不能仅靠这个(因为‘的’这样的词在文章中出现的概率必然很高，但显然它不重要)，所以还要用idf加权，也就是说该词在其他文档出现频率过高的话重要性会降低。

## MR Design

1. counting occurrence num of each word in each document：
   * wordcount in the all separate articles—> Job1
   * mapper:
     * Input: (docName, contents)
     * Output: ((word, docName), 1)
   * reducer:
     * input: ((word, docName), 1)
     * Output: ((word, docName), n)
   * Combiner: the same as reducer
2. counting TF:
   * Mapper:
     * input: ((word, docName), n)
     * output: (docName, (word, n))
   * Reducer:
     * function: count N, that is sum(nij)
     * input: (docName, (word, n))
     * output: (docName, (word, n / N)), that is (docName, (word, TF))
     * u need to cache all (word, n)  for one docName in memory.   —> this is workable. **But** u should save memory in map&reduce tasks to imporve the efficiency of shuffle.
3. counting IDF:
   * Mapper:
     * input: (docName, (word, n / N))
     * output: (word, (docName, n / N))
   * Reducer:
     * function: count (word, docName)
     * Input: (word, (docName, n / N))
     * Output: ((word, docName), n / N, m)  —> ((word, docName), (n / N) * log(D / m))
     * u need to cache all (docName, n / N) for one word in memory. Will this exceed the memory limit?!

## Improvement

* Hadoop is not good at handling smaller files.    —>    Use **SequenceFile**, the key is filename, the value is file content.
* IDF = log(Dw / (Dt + 1))  —> to avoid denominator to be zero.
* **Verifying the stopwords** in dictionary
* Use RegEx to select only words, removing punctuation and other data anomalies.

## FYI

* [wikipedia-tfidf](https://en.wikipedia.org/wiki/Tf–idf)