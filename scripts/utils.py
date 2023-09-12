import nltk
from glob import glob

# returns the list of words the "line" string is made of
def tokenize(line):
    tokens = nltk.word_tokenize(line)

    # splits words separated by '
    i=0
    while i < len(tokens):
        if '\'' in tokens[i]:
            parts = tokens[i].split("'")
            tokens.pop(i)
            tokens.insert(i, parts[0])
            tokens.insert(i+1, parts[1])
        i += 1

    # removes words containting numbers or symbols
    tokens = [word.lower() for word in tokens if word.isalpha()]

    return tokens


# reads spark's output, 
# sc - sparkcontext
# dir_path - directory's path
# returns an RDD containing the read file
def readFiles(sc, dir_path):
    rdd = sc.parallelize([])

    for path in glob(dir_path+"/part-00***"):
        rdd = rdd.union(sc.textFile(path))

    return rdd
