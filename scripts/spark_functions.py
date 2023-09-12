#from __future__ import print_function
#import sys
from pyspark import SparkContext
from utils import *
import math

SPARK_CONTEXT_MEMORY = '12G'

def distribution(corpus_path, output_path):

    sc = SparkContext(appName="Distribution").getOrCreate()

    rdd_corpora = sc.textFile(corpus_path)

    rdd_tokens = rdd_corpora.flatMap(tokenize)

    rdd_distribution = rdd_tokens.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    rdd_distribution_sorted = rdd_distribution.sortBy(lambda x : x[1], ascending=False)     # x = (token, count)

    rdd_to_file_format = rdd_distribution_sorted.map(lambda x : x[0] + ', ' + str(x[1]))
    rdd_to_file_format.saveAsTextFile(output_path)

    sc.stop()


def whitelist(paisa_min_frequency, wiki_min_frequency, paisa_dist_path, wiki_dist_path, output_path):
    
    sc = SparkContext(appName="Whitelist").getOrCreate()

    rdd_paisa_dist_str = readFiles(sc, paisa_dist_path)
    rdd_wiki_dist_str = readFiles(sc, wiki_dist_path)

    rdd_paisa_dist = rdd_paisa_dist_str.map(lambda line : (line.split(', ')[0], int(line.split(', ')[1])))
    rdd_wiki_dist = rdd_wiki_dist_str.map(lambda line : (line.split(', ')[0], int(line.split(', ')[1])))

    def char_filter(word):
        lowercase_alphabets = [chr(i) for i in range(97, 123)] + ['è', 'é', 'à', 'ù', 'ì', 'ò']
        for char in word:
            if char not in lowercase_alphabets:
                return False
        return True
    rdd_paisa_dist_filtered = rdd_paisa_dist.filter(lambda x : x[1] >= paisa_min_frequency and char_filter(x[0])).map(lambda x : x[0])
    rdd_wiki_dist_filtered  = rdd_wiki_dist.filter (lambda x : x[1] >= wiki_min_frequency  and char_filter(x[0])).map(lambda x : x[0])

    rdd_whitelist = rdd_paisa_dist_filtered.intersection(rdd_wiki_dist_filtered)

    rdd_whitelist.coalesce(1).saveAsTextFile(output_path)

    sc.stop()


def fix_whitelist(whitelist_path, small_corpora_paths, output_path):
    whitelist_file = open(whitelist_path)
    whitelist = {line[:-1]: 1 for line in whitelist_file}
    whitelist_file.close()

    for corpora_path in small_corpora_paths:
        tokens = []
        for path in glob(corpora_path+"/part-00***"):
            distribution_file = open(path)
            tokens += [line.split(', ')[0] for line in distribution_file]
            distribution_file.close()

        new_tokens_count = 0
        for token in tokens:
            if whitelist.get(token) == None:
                whitelist[token] = 1
                new_tokens_count += 1
        print(f'{new_tokens_count} new tokens added.')

    f = open(output_path, 'w+')
    for k, _ in whitelist.items():
        f.write(f'{k}\n')
    f.close()


def tokenization(stopwords_path, whitelist_path, corpus_path, output_path):
    # Load stopwords list from file
    file = open(stopwords_path)
    stopwords = [word[:-1] for word in file]
    file.close()

    # Load whitelist and create the dictionary
    file = open(whitelist_path)
    dictionary = {line[:-1]: index+1 for index, line in enumerate(file)}
    file.close()

    sc = SparkContext(appName="Tokenization").getOrCreate()

    # Setting SparkContext's variable to allow more memory usage
    sc._conf.set('spark.executor.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.maxResultSize', SPARK_CONTEXT_MEMORY)

    # Load corpora as RDD
    rdd_corpora = sc.textFile(corpus_path)

    rdd_tokens = rdd_corpora.map(tokenize)

    # Removing stopwords
    def filter(tokens_in_list):
        l = []
        for token in tokens_in_list:
            if token not in stopwords:
                l.append(token)
        return l
    rdd_nostopwords = rdd_tokens.map(filter)

    # Tokenization
    def tokenization_map(line, dictionary):
        l = []
        for token in line:
            if dictionary.get(token) != None:
                l.append(dictionary[token])
            else:
                l.append(0)   # unkown token
        return l
    rdd_tokenization = rdd_nostopwords.map(lambda tokens_in_list: tokenization_map(tokens_in_list, dictionary))

    def to_str(token_list):
        stringa = ''
        for token in token_list:
            stringa += str(token) + ' '
        return stringa
    rdd_to_file_format = rdd_tokenization.map(to_str)
    rdd_to_file_format.saveAsTextFile(output_path)

    sc.stop()


def co_occurrences(tokenized_corpus_path, window_size, output_path):
    
    sc = SparkContext(appName="Co_occurrences").getOrCreate()

    # Setting SparkContext's variable to allow more memory usage
    sc._conf.set('spark.executor.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.maxResultSize', SPARK_CONTEXT_MEMORY)

    rdd_tokenized_corpus_string = readFiles(sc, tokenized_corpus_path)

    rdd_tokenized_corpus = rdd_tokenized_corpus_string.map(lambda token_str_list : [int(token_str) for token_str in token_str_list.split()])

    def co_occ(lista, window_size):
      co_occ_list = []
      for index, token in enumerate(lista):

        if index == len(lista) -1:
          break
        
        if token != 0:    # known token
          for window_index in range(index + 1, min(len(lista), index + window_size)):
            token2 = lista[window_index]
            if token2 != 0 and token != token2:
              if token > token2:
                co_occ_list.append(((token2, token), 1))
              else:
                co_occ_list.append(((token, token2), 1))

      return co_occ_list
    rdd_pairs = rdd_tokenized_corpus.flatMap(lambda phrase: co_occ(phrase, window_size))

    rdd_co_occ = rdd_pairs.reduceByKey(lambda x, y: x + y)#.sortByKey()

    rdd_to_file_format = rdd_co_occ.map(lambda x : str(x[0][0]) + ' ' + str(x[0][1]) + ' ' + str(x[1]))
    rdd_to_file_format.saveAsTextFile(output_path)

    sc.stop()


def create_model(whitelist_path, cooc_paths, dist_paths, corpora_weights, output_path):

    sc = SparkContext(appName="PMI").getOrCreate()
    
    # Setting SparkContext's variable to allow more memory usage
    sc._conf.set('spark.executor.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.memory', SPARK_CONTEXT_MEMORY)
    sc._conf.set('spark.driver.maxResultSize', SPARK_CONTEXT_MEMORY)

    # read co-occurrences from files
    rdd_co_occ_list_str = []
    for name in cooc_paths:
        rdd_co_occ_list_str.append(readFiles(sc, name))

    # convert strings to pairs
    def co_oc_str_to_pair(stringa):
        lista = stringa.split()
        return ((int(lista[0]), int(lista[1])), int(lista[2]))
    rdd_co_occ_list = []
    for rdd in rdd_co_occ_list_str:
        rdd_co_occ_list.append(rdd.map(co_oc_str_to_pair))

    # multiply each co-occurrence for its corpora weight
    rdd_weighted_co_occ_list = []
    for index, _ in enumerate(corpora_weights):
        rdd = rdd_co_occ_list[index].map(lambda x : ((x[0][0], x[0][1]), x[1] * corpora_weights[index]))
        rdd_weighted_co_occ_list.append(rdd)

    # sum them up
    rdd_co_occ = sc.union(rdd_weighted_co_occ_list).reduceByKey(lambda x, y: x + y)

    # read distribution from file
    rdd_dist_list_str = []
    for name in dist_paths:
        rdd_dist_list_str.append(readFiles(sc,name))

    # convert strings to pairs
    def dist_str_to_pair(stringa):
        lista = stringa.split()
        return (lista[0][:-1], int(lista[1]))
    rdd_dist_list = []
    for rdd in rdd_dist_list_str:
        rdd_dist_list.append(rdd.map(dist_str_to_pair))
     
    # sum them up
    rdd_dist = sc.union(rdd_dist_list).reduceByKey(lambda x, y: x + y)

    # read whitelist from file
    f = open(whitelist_path, 'r')
    whitelist = []
    for line in f:
        whitelist.append(line[:-1])
    f.close()

    # create dictionary and inverted dictionary from whitelist
    dictionary = {key: (value + 1) for value, key in enumerate(whitelist)}

    # substitute string token with int token
    rdd_dist_token = rdd_dist.map(lambda x : (dictionary.get(x[0]), x[1])).filter(lambda x: x[0] != None)

    # map so that the first token is the key
    rdd_co_occ_map1 = rdd_co_occ.map(lambda x : (x[0][0], (x[0][1], x[1])))

    # join with distribution
    rdd_join1 = rdd_co_occ_map1.leftOuterJoin(rdd_dist_token)

    # map so that the second token is the key
    rdd_co_occ_map2 = rdd_join1.map(lambda x : (x[1][0][0], (x[0], x[1][0][1], x[1][1])))

    # join with distribution
    rdd_join2 = rdd_co_occ_map2.leftOuterJoin(rdd_dist_token)

    # calculate PMI values
    rdd_pmi = rdd_join2.map(lambda x : ((x[1][0][0], x[0]), math.log(x[1][0][1] / (x[1][0][2] * x[1][1]))))

    rdd_to_file_format = rdd_pmi.map(lambda x : str(x[0][0]) + ' ' + str(x[0][1]) + ' ' + str(x[1]))
    rdd_to_file_format.saveAsTextFile(output_path)

    sc.stop()
