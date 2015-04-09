from gensim.models import word2vec
# Import the built-in logging module and configure it so that Word2Vec
# creates nice output messages
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# Set values for various parameters
num_features = 30  # Word vector dimensionality: LOW DIMENSIONALITY FOR TYPE SIMILARITY
min_word_count = 40  # Minimum word count
num_workers = 4  # Number of threads to run in parallel
context = 10  # Context window size
downsampling = 1e-3  # Down-sample setting for frequent words

text = word2vec.LineSentence('wiki_text_total_spotlight_specific.txt')
model = word2vec.Word2Vec(text, workers=num_workers, size=num_features, min_count=min_word_count,
                 window=context, sample=downsampling, sg=0)
model.init_sims(replace=True)
model.save(str(num_features)+"features"+str(min_word_count)+"minwords"+str(context)+"context"+"_wiki_specific")