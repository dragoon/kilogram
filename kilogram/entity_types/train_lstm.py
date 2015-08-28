import sys
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras.layers.recurrent import LSTM
from gensim.models import word2vec

CONTEXT_SIZE = 2


def get_features(sentence, index):
    vector = np.array([])
    vector = vector.reshape((0, 128))
    # get the context and create a training sample
    for j in range(index-CONTEXT_SIZE, index+1+CONTEXT_SIZE):
        if j != index:
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
    model.add(Dense(128, 1))
    model.add(Activation('sigmoid'))

    print 'Compiling LSTM model'
    # try using different optimizers and different optimizer configs
    model.compile(loss='binary_crossentropy', optimizer='adam', class_mode="binary")

    NUM_SAMPLES = 100000

    import numpy as np
    X_train = np.empty((NUM_SAMPLES, CONTEXT_SIZE*2, 128))
    y_train = np.empty((NUM_SAMPLES,))
    #y_train = np.empty((NUM_SAMPLES, 128))
    data = open(sys.argv[2])
    entity_index = 0
    true_label_index = 0
    print 'Collecting training samples'
    out = open('dbpedia_writier_100K_sample.txt', 'w')
    for line in data:
        if entity_index > NUM_SAMPLES - 1:
            break
        words = line.split()
        # pad words with the context size
        words = ['--NONE--']*CONTEXT_SIZE + words + ['--NONE--']*CONTEXT_SIZE
        for i, word in enumerate(words):
            if word.startswith('<dbpedia:'):
                if word not in word2vec_model:
                    continue
                # true_vector = word2vec_model[word]
                # y_train = np.append(y_train, [true_vector], axis=0)
                label = int(word == '<dbpedia:Writer>')
                if label:
                    true_label_index += 1
                elif true_label_index*2 < entity_index:
                    continue
                out.write(' '.join(words[i-CONTEXT_SIZE:i+1+CONTEXT_SIZE])+'\n')
                y_train[entity_index] = label
                X_train[entity_index] = get_features(words, i)
                entity_index += 1
                if entity_index > NUM_SAMPLES - 1:
                    break
    data.close()
    out.close()


    def balanced_subsample(x, y, subsample_size=1.0):

        class_xs = []
        min_elems = None

        for yi in np.unique(y):
            elems = x[(y == yi)]
            class_xs.append((yi, elems))
            if min_elems == None or elems.shape[0] < min_elems:
                min_elems = elems.shape[0]

        use_elems = min_elems
        if subsample_size < 1:
            use_elems = int(min_elems*subsample_size)

        xs = []
        ys = []

        for ci,this_xs in class_xs:
            if len(this_xs) > use_elems:
                np.random.shuffle(this_xs)

            x_ = this_xs[:use_elems]
            y_ = np.empty(use_elems)
            y_.fill(ci)

            xs.append(x_)
            ys.append(y_)

        xs = np.concatenate(xs)
        ys = np.concatenate(ys)

        return xs, ys

    print 'Balancing training samples'
    #X_train, y_train = balanced_subsample(X_train, y_train)

    model.fit(X_train, y_train, batch_size=8, nb_epoch=10, validation_split=0.1, show_accuracy=True)
    model.save_weights('model_lstm.bin')
