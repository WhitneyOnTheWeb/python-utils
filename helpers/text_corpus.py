from collections import defaultdict
from gensim import corpora, models
from scipy.spatial import distance
from scipy import sparse, io
import numpy as np
import nltk.corpus
import nltk.stem

nltk.download('stopwords')
nltk.download('wordnet')


class TextCorpus(corpora.textcorpus.TextCorpus):
    def get_texts(self): return self.input
    def __len__(self): return len(self.input)

class PreProcessText(object):
    def GetWords(texts):
        texts = [w.split() for w in texts]
        texts = [list(map(lambda w: w.lower(), w)) for w in texts]
        return texts

    def ClearNoiseFromWords(texts):
        filter_symbols = '+-^.,/~\\_*$&#=?!()"[]{}<>:;@©℗®™|'
        alphanumeric = '[0-9a-zA-Z]'
        texts = [list(filter(lambda s: not len(set(filter_symbols) & set(s)), w))
                    for w in texts]
        return texts

    def RemoveStopWords(texts, update):
        stopwords = set(nltk.corpus.stopwords.words('english'))
        stopwords.update(update)

        texts = [filter(lambda w: w not in stopwords, w)
                    for w in texts]
        return texts

    def RemoveShortWords(texts):
        texts = [list(filter(lambda w: len(w) > 3, w))
                    for w in texts]
        return texts

    def StemWords(texts):
        english_stemmer = nltk.stem.SnowballStemmer('english')
        texts = [list(map(english_stemmer.stem, w))
                    for w in texts]
        return texts
    
    def LemmatizeWords(texts):
        lemmatizer = nltk.stem.WordNetLemmatizer()
        texts = [list(map(lemmatizer.lemmatize, w))
                    for w in texts]
        return texts

    def RemoveCommonWords(texts):
        textc, too_common, usage, limit = PreProcessText.Common(texts)
        texts = [list(filter(lambda w: w not in too_common, w))
                    for w in textc]
        return texts, too_common, usage, limit

    def Common(texts):
        # Calculate word usage
        usage = defaultdict(int)
        textc = [] #Preserve uncommon words

        for doc in texts:
            textc.append(list(doc))
            for word in set(doc):
                usage[word] += 1

        limit = len(texts) / 10

        too_common = [w for w in usage if usage[w] > limit]
        too_common = set(too_common)
        return textc, too_common, usage, limit


class Topics(object):
    def __init__(self, name, model, corpus, vocab):
        self.name = name
        self.model = model
        self.corpus = corpus
        self.vocab = vocab
        self.topics = self.TopicVectors()
        self.distances = None

    def set_distances(self):
        self.distances = self.DistanceVectors()

    def TopicVectors(self):
        topics = np.zeros((len(self.corpus)
                         , self.model.num_topics))
        for di, doc in enumerate(self.corpus):
            for ti, v in self.model[doc]:
                topics[di, ti] += v
        print('Topic Vectors Created')
        return topics

    def DistanceVectors(self):
        # Compute pairwise distances between all topics in corpus
        distances = distance.squareform(distance.pdist(self.topics))
        large = distances.max() + 1
        for i in range(len(distances)):
            distances[i, i] = large
        print('Distance Vectors Created')
        return distances

    def most_similar_to(self, raw, docid):
        if self.distances == None:
            print('Calculating Distances...')
            self.set_distances()
        line = '=' * 80

        print('Document ID: [{0}]:\n{2}\n{1}'\
              .format(docid, raw[docid], line))
        print(line + '\n\n\n')
        print('Most Similar Document ID: [{0}]:\n{2}\n{1}'\
              .format(self.distances[docid].argmin()\
            , raw[self.distances[docid].argmin()], line))
        return self.distances[docid].argmin()