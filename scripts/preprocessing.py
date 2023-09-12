from spark_functions import *

def __name__='__main__':
    
    distribution(corpus_path='../corpus/corpuscollocazioni.txt', 
                 output_path='../spark_results/dist_collocazioni')
    distribution(corpus_path='../corpus/paisa_fixed.txt', 
                 output_path='../spark_results/dist_paisa')
    distribution(corpus_path='../corpus/polirematics.txt', 
                 output_path='../spark_results/dist_polirematics')
    distribution(corpus_path='../corpus/songs_corpus.txt', 
                 output_path='../spark_results/dist_songs')
    distribution(corpus_path='../corpus/sayings_corpus.txt', 
                 output_path='../spark_results/dist_sayings')
    distribution(corpus_path='../corpus/wiki_fixed.txt', 
                 output_path='../spark_results/dist_wiki')
    distribution(corpus_path='../corpus/wiki_titles.txt', 
                 output_path='../spark_results/dist_wiki_titles')
    
    
    whitelist(paisa_min_frequency=4, 
              wiki_min_frequency=100, 
              paisa_dist_path='../spark_results/dist_paisa',
              wiki_dist_path='../spark_results/dist_wiki',
              output_path='../spark_results/whitelist')
    
    small_corpora_paths = ['../spark_results/dist_collocazioni',
                           '../spark_results/dist_polirematics',
                           '../spark_results/dist_sayings',
                           '../spark_results/dist_songs']

    fix_whitelist(whitelist_path='../spark_results/whitelist/part-00000', 
                  small_corpora_paths=small_corpora_paths,
                  output_path='../corpus/whitelist_fixed.txt')


    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/corpuscollocazioni.txt', 
                 output_path='../spark_results/token_collocazioni')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/paisa_fixed.txt', 
                 output_path='../spark_results/token_paisa')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/polirematics.txt', 
                 output_path='../spark_results/token_polirematics')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/sayings_corpus.txt', 
                 output_path='../spark_results/token_sayings')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/songs_corpus.txt', 
                 output_path='../spark_results/token_songs')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/wiki_fixed.txt', 
                 output_path='../spark_results/token_wiki')
    tokenization(stopwords_path='../corpus/stopwords', 
                 whitelist_path='../corpus/whitelist_fixed.txt', 
                 corpus_path='../corpus/wiki_titles.txt', 
                 output_path='../spark_results/token_wiki_titles')
    
    
    co_occurrences(tokenized_corpus_path='../spark_results/token_collocazioni',
                   window_size=3, 
                   output_path='../spark_results/co_occ_collocazioni')
    co_occurrences(tokenized_corpus_path='../spark_results/token_paisa',
                   window_size=3, 
                   output_path='../spark_results/co_occ_paisa')
    co_occurrences(tokenized_corpus_path='../spark_results/token_polirematics',
                   window_size=3, 
                   output_path='../spark_results/co_occ_polirematics')
    co_occurrences(tokenized_corpus_path='../spark_results/token_sayings',
                   window_size=3, 
                   output_path='../spark_results/co_occ_sayings')
    co_occurrences(tokenized_corpus_path='../spark_results/token_songs',
                   window_size=3, 
                   output_path='../spark_results/co_occ_songs')
    co_occurrences(tokenized_corpus_path='../spark_results/token_wiki',
                   window_size=3, 
                   output_path='../spark_results/co_occ_wiki')
    co_occurrences(tokenized_corpus_path='../spark_results/token_wiki_titles',
                   window_size=3, 
                   output_path='../spark_results/co_occ_wiki_titles')
               
               
    cooc_paths = ["../spark_results/co_occ_collocazioni",
                  "../spark_results/co_occ_paisa",
                  "../spark_results/co_occ_songs",
                  "../spark_results/co_occ_sayings",
                  "../spark_results/co_occ_polirematics",
                  "../spark_results/co_occ_wiki",
                  "../spark_results/co_occ_wiki_titles"]
    dist_paths = ["../spark_results/dist_collocazioni",
                  "../spark_results/dist_paisa",
                  "../spark_results/dist_songs",
                  "../spark_results/dist_sayings",
                  "../spark_results/dist_polirematics",
                  "../spark_results/dist_wiki",
                  "../spark_results/dist_wiki_titles"]
    corpora_weights = [100,1,50,100,200,1,25]

    create_model(whitelist_path='../corpus/whitelist_fixed.txt',
                 cooc_paths=cooc_paths, 
                 dist_paths=dist_paths, 
                 corpora_weights=corpora_weights, 
                 output_path='../spark_results/PMI')           

