import sys
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.recurrent import LSTM
from gensim.models import word2vec

CONTEXT_SIZE = 5


def get_features(sentence, index):
    vector = np.array([])
    vector = vector.reshape((0, 128))
    # get the context and create a training sample
    for j in range(index-CONTEXT_SIZE, index):
        try:
            vector = np.append(vector, [word2vec_model[sentence[j]]], axis=0)
        except:
            vector = np.append(vector, [[0]*128], axis=0)
    return vector

if __name__ == "__main__":
    print 'Loading word2vec model'
    word2vec_model = word2vec.Word2Vec.load(sys.argv[1])

    model = Sequential()
    model.add(LSTM(128, 128))  # try using a GRU instead, for fun
    model.add(Dropout(0.5))
    model.add(Dense(128, 128))
    model.add(Activation('softmax'))

    print 'Compiling LSTM model'
    # try using different optimizers and different optimizer configs
    model.compile(loss='binary_crossentropy', optimizer='adam')

    NUM_SAMPLES = 100000

    import numpy as np
    X_train = np.empty((NUM_SAMPLES, CONTEXT_SIZE, 128))
    y_train = np.empty((NUM_SAMPLES, 128))
    data = open(sys.argv[2])
    entity_index = 0
    true_label_index = 0
    print 'Collecting training samples'
    out = open('sample.txt', 'w')
    for i, line in enumerate(data):
        if i > NUM_SAMPLES:
            break
        words = line.strip().split()
        # pad words with the context size
        words = ['--NONE--']*CONTEXT_SIZE + words
        out.write(' '.join(words)+'\n')
    data.close()
    out.close()

    model.fit(X_train, y_train, batch_size=8, nb_epoch=10)
    model.save_weights('language_model_lstm.bin')
