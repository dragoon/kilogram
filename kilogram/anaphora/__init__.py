from __future__ import division
from gensim.models import word2vec
from numpy import dot, vstack
import numpy as np
from gensim import matutils
w2v_model = word2vec.Word2Vec.load('/Users/dragoon/Downloads/300features_40minwords_10context')
w2v_model.init_sims()

from collections import defaultdict


class CorefCluster(object):
    # groups of mentions, can be headword or some other grouping
    mention_groups = None
    non_noun_groups = None
    coref_cluster_id = None

    def __init__(self):
        self.non_noun_groups = defaultdict(lambda: MentionGroup())
        self.mention_groups = defaultdict(lambda: MentionGroup())

    def __repr__(self):
        return 'CorefCluster:' + str(dict(self.mention_groups))

    def add_mention_group(self, mention_group):
        # change mention cluster_ids
        for mention in mention_group.mentions:
            mention.coref_cluster_id = self.coref_cluster_id
        if mention_group.head_lemma in self.mention_groups:
            self.mention_groups[mention_group.head_lemma].mentions.extend(mention_group.mentions)
        else:
            self.mention_groups[mention_group.head_lemma] = mention_group

    def add_mention(self, mention):
        if mention.head_pos in ('PRP', 'PRP$', 'DT'):
            self.non_noun_groups[mention.head_lemma].add_mention(mention)
        else:
            self.mention_groups[mention.head_lemma].add_mention(mention)
        self.coref_cluster_id = mention.coref_cluster_id

    def __len__(self):
        return len(self.mention_groups)


class MentionGroup(object):
    mentions = None
    head_lemma = None

    def __init__(self):
        self.mentions = []

    def __repr__(self):
        return str(self.head_lemma) + ": " + str(len(self.mentions))

    def add_mention(self, mention):
        self.mentions.append(mention)
        self.head_lemma = mention.head_lemma


class Mention(object):
    mention = None
    sent_id = None
    mention_id = None
    start_i = None
    end_i = None
    head_lemma = None
    head_pos = None
    ner_type = None
    coref_cluster_id = None
    gold_coref_id = None

    def __init__(self, date_tuple):
        self.sent_id = date_tuple[2]
        self.mention_id = date_tuple[3]
        self.start_i = date_tuple[4]
        self.end_i = date_tuple[5]
        self.mention = date_tuple[6]
        self.ner_entity = date_tuple[7]
        self.head_lemma = date_tuple[8]
        self.head_pos = date_tuple[9]
        self.coref_cluster_id = date_tuple[10]
        self.gold_coref_id = date_tuple[11]

    def __unicode__(self):
        return self.mention

    def __repr__(self):
        return self.mention


class NoWordInVocabularyError(Exception):
    pass


class EmptyGoldCluster(Exception):
    pass


def parse_corefs_data(filename):
    corefs_data = open(filename).readlines()
    corefs_data = [x.strip().split('\t') for x in corefs_data]
    # remove header
    del corefs_data[0]

    # dict of meta information about auto cluster,
    coref_clusters = defaultdict(lambda: defaultdict(lambda: CorefCluster()))
    for data_tuple in corefs_data:
        coref_clusters[(data_tuple[0], data_tuple[1])][data_tuple[10]].add_mention(Mention(data_tuple))
    return coref_clusters


class FeatureExtractor(object):

    def __init__(self, coref_clusters):
        self.skipped_empty_cluster = 0
        self.skipped_no_word = 0
        self.skipped_empty_gold = 0
        self.passed = 0
        self.coref_clusters = coref_clusters

    @staticmethod
    def get_ngram_vector(ngram):
        words = [word for word in ngram.split() if word in w2v_model.vocab]
        if not words:
            return None
        vectors = vstack(w2v_model.syn0norm[w2v_model.vocab[word].index] for word in words)
        mean = matutils.unitvec(vectors.mean(axis=0))
        return mean

    @staticmethod
    def get_feature_vector(mentions, test_mention_group):

        # START: VECTORS
        test_vector = FeatureExtractor.get_ngram_vector(test_mention_group.head_lemma)
        if test_vector is None:
            raise NoWordInVocabularyError()
        mention_ngrams = set(mention.head_lemma for mention in mentions)
        other_vectors = [FeatureExtractor.get_ngram_vector(mention) for mention in mention_ngrams]
        other_vectors = filter(lambda x: x is not None, other_vectors)
        if len(other_vectors) == 0:
            raise EmptyGoldCluster()
        other_vectors = vstack(other_vectors)
        mean = matutils.unitvec(other_vectors.mean(axis=0))
        similarity = dot(test_vector, mean)
        similarities = [dot(matutils.unitvec(x), test_vector) for x in other_vectors]
        # END: VECTORS

        # START: SYNTACTIC
        sent_ids = set(int(x.sent_id) for x in mentions)
        test_sent_ids = set(int(x.sent_id) for x in test_mention_group.mentions)
        distances = []
        for sent_id in sent_ids:
            for test_sent_id in test_sent_ids:
                distances.append(abs(sent_id - test_sent_id))
        # END: SYNTACTIC


        feature_vector = [similarity]#, min(similarities), max(similarities),
                          #min(distances), max(distances), np.mean(distances)]
        #feature_vector.extend(list(test_vector))
        feature_vector.extend(list(test_vector - mean))
        return feature_vector

    def get_features(self):
        feature_vectors = []
        labels = []
        for doc_coref_clusters in self.coref_clusters.values():
            mention_groups = []
            for coref_cluster in doc_coref_clusters.values():
                for mention_group in coref_cluster.mention_groups.values():
                    mention_groups.append(mention_group)
            for coref_cluster in doc_coref_clusters.values():
                for mention_group in mention_groups:
                    local_fv, local_lab = self._get_features(coref_cluster, mention_group)
                    feature_vectors.extend(local_fv)
                    labels.extend(local_lab)
        print 'Skipped Small Cluster:', self.skipped_empty_cluster
        print 'Skipped Empty Gold Cluster:', self.skipped_empty_gold
        print 'Skipped No Test Word Vector:', self.skipped_no_word
        print 'Passed:', self.passed
        return feature_vectors, labels

    def _get_features(self, coref_cluster, mention_group):
        feature_vectors = []
        labels = []
        mention_gold_ids = [mention.gold_coref_id for mention in mention_group.mentions]

        gold_clusters = defaultdict(list)
        # populate gold clusters
        for x_group in coref_cluster.mention_groups.values():
            for mention in x_group.mentions:
                gold_clusters[mention.gold_coref_id].append(mention)
        # generate features
        for gold_cluster_id, gold_mentions in gold_clusters.items():
            label = int(gold_cluster_id in mention_gold_ids)
            if label == 1:
                # exclude mention from gold_values if same cluster
                gold_mentions = [x for x in gold_mentions if x.head_lemma != mention_group.head_lemma]
            if len(gold_mentions) == 0:
                self.skipped_empty_cluster += 1
                continue
            try:
                feature_vector = FeatureExtractor.get_feature_vector(gold_mentions, mention_group)
            except NoWordInVocabularyError:
                self.skipped_no_word += 1
                continue
            except EmptyGoldCluster:
                self.skipped_empty_gold += 1
                continue
            self.passed += 1
            labels.append(label)
            feature_vectors.append(feature_vector)
        return feature_vectors, labels


class ClusterReassigner(object):

    def __init__(self, coref_clusters, clf):
        self.clf = clf
        self.skipped = 0
        self.internal_precision = []
        self.internal_recall = 0
        self.coref_clusters = coref_clusters

    def _doesnt_match(self, coref_cluster):
        mention_groups = coref_cluster.mention_groups
        if len(mention_groups) < 2:
            return None
        mention_classes = self._get_mention_classes(mention_groups)
        if not mention_classes:
            return None
        lowest = mention_classes[-1]
        # handle two-entity tie, same probability
        if len(mention_classes) == len([1 for x in mention_classes if x[0] > 0.5]) == 2:
            # TODO: break the min tie
            lowest = min(mention_classes, key=lambda x: len(x[1].mentions))
        if lowest[0] > 0.5:
            return lowest[1]
        return None

    def _get_mention_classes(self, mention_groups):
        mention_group_classes = []
        mention_groups = mention_groups.values()
        for i, mention_group in enumerate(mention_groups):

            other_mentions = [mention for x_group in mention_groups
                              for mention in x_group.mentions
                              if mention.head_lemma != mention_group.head_lemma]
            try:
                features = FeatureExtractor.get_feature_vector(other_mentions, mention_group)
            except:
                self.skipped += 1
                continue
            class1, class2 = self.clf.predict_proba(features)[0]
            #print mention.gold_coref_id, class1, class2
            mention_group_classes.append((class1, mention_group))
        mention_group_classes.sort(key=lambda x: x[0])
        return mention_group_classes

    def evaluate_internal(self):
        for key, doc_clusters in self.coref_clusters.iteritems():
            for coref_cluster in doc_clusters.values():
                if len(set(
                        [y.gold_coref_id for mention_group in coref_cluster.mention_groups.values()
                         for y in mention_group.mentions])) > 1:
                    self.internal_recall += 1
                mention_groups = coref_cluster.mention_groups
                if len(mention_groups) < 2:
                    continue
                mention_classes = self._get_mention_classes(mention_groups)
                if not mention_classes:
                    continue
                lowest = mention_classes.pop(-1)
                other_gold_ids = set([y.gold_coref_id for _, mention_group in mention_classes
                                      for y in mention_group.mentions])
                lowest_gold_ids = set([mention.gold_coref_id for mention in lowest[1].mentions])
                if lowest[0] > 0.5:
                    # need to re-assign at least one entity
                    self.internal_precision.append(int(not lowest_gold_ids.intersection(other_gold_ids)))
        print 'Total identified cases:', len(self.internal_precision)
        print 'Skipped:', self.skipped
        print 'Precision:', sum(self.internal_precision)/len(self.internal_precision)
        print 'Recall: ', sum(self.internal_precision)/self.internal_recall, self.internal_recall
        # reset
        self.skipped = 0
        self.internal_recall = 0
        self.internal_precision = []

    def generate_external_file(self):
        import copy
        copy_coref_clusters = copy.deepcopy(self.coref_clusters)
        for key, doc_clusters in copy_coref_clusters.iteritems():
            non_matching_groups = []
            while True:
                for auto_cluster_id, coref_cluster in doc_clusters.iteritems():
                    non_matching = self._doesnt_match(coref_cluster)
                    if non_matching:
                        del coref_cluster.mention_groups[non_matching.head_lemma]
                        non_matching_groups.append(non_matching)
                if len(non_matching_groups) == 0:
                    break
                while len(non_matching_groups) > 0:
                    mention_group = non_matching_groups.pop()
                    # try to assign group to a new cluster
                    matched = False
                    for coref_cluster in doc_clusters.values():
                        coref_cluster_mentions = \
                            [mention for x_group in coref_cluster.mention_groups.values()
                             for mention in x_group.mentions]
                        try:
                            features = FeatureExtractor.get_feature_vector(coref_cluster_mentions, mention_group)
                        except:
                            continue
                        if self.clf.predict(features)[0] == 1:
                            # add to the current cluster
                            coref_cluster.add_mention_group(mention_group)
                            matched = True
                            break
                    if not matched:
                        # create new cluster
                        print 'NEW CLUSTER'
                        new_cluster_id = str(max([int(x) for x in doc_clusters.keys()]) + 1)
                        doc_clusters[new_cluster_id].coref_cluster_id = new_cluster_id
                        doc_clusters[new_cluster_id].add_mention_group(mention_group)
        return copy_coref_clusters

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import *
from sklearn.cross_validation import cross_val_score
from sklearn.utils import resample

gold_corefs_data = parse_corefs_data('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-train-gold-mentions.txt')
feature_vectors, labels = FeatureExtractor(gold_corefs_data).get_features()

feature_vectors_labels = zip(feature_vectors, labels)
positive = [x for x in feature_vectors_labels if x[1] == 1]
negative = resample([x for x in feature_vectors_labels if x[1] == 0], n_samples=len(positive))
feature_vectors_labels = positive+negative
feature_vectors, labels = zip(*feature_vectors_labels)

print 'Positive Labels:', len(filter(lambda x: x == 1, labels))
print 'Negative Labels:', len(filter(lambda x: x == 0, labels))

clf_extra_trees = ExtraTreesClassifier(n_estimators=50)
clf_log_reg = LogisticRegression()

# QUICK EVALUATION
print cross_val_score(clf_extra_trees, feature_vectors, labels, cv=5)
print cross_val_score(clf_log_reg, feature_vectors, labels, cv=5)

# Fit before evaluation
clf_extra_trees.fit(feature_vectors, labels)

reassigner = ClusterReassigner(gold_corefs_data, clf_extra_trees)
print 'Evaluate on Train data:'
reassigner.evaluate_internal()
print 'Evaluate on Test data:'
reassigner.coref_clusters = parse_corefs_data('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-dev-gold-mentions.txt')
reassigner.evaluate_internal()

print 'Generating External File:'
reassigner.coref_clusters = parse_corefs_data('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-dev.txt')
new_coref_clusters = reassigner.generate_external_file()


def generate_new_mentions(new_coref_clusters):
    new_mentions = defaultdict(list)
    for doc_id, doc_coref_clusters in new_coref_clusters.iteritems():
        doc_id, par_id = doc_id
        for cluster_id, coref_cluster in doc_coref_clusters.iteritems():
            for mention_group in coref_cluster.mention_groups.itervalues():
                for mention in mention_group.mentions:
                    key = (doc_id, par_id, int(mention.sent_id), mention.start_i)
                    new_mentions[key].append((cluster_id, mention.end_i))
            for mention_group in coref_cluster.non_noun_groups.itervalues():
                for mention in mention_group.mentions:
                    key = (doc_id, par_id, int(mention.sent_id), mention.start_i)
                    new_mentions[key].append((cluster_id, mention.end_i))
    return new_mentions


def generate_conll_corefs_file(new_mentions):
    old_corefs_data = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conll-dev.predicted.txt')
    new_corefs_file = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conll-dev.predicted.new.txt', 'w')

    sent_id = 0
    end_clusters = defaultdict(list)
    for line_num, line in enumerate(old_corefs_data):
        line = line.strip()
        if line.startswith(('#begin', '#end')):
            sent_id = 0
            new_corefs_file.write(line+'\n')
        elif len(line) == 0:
            sent_id += 1
            new_corefs_file.write(line+'\n')
        else:
            line = line.split('\t')
            doc_id, par_id, word_num = line[:3]
            key = (doc_id, par_id, sent_id, word_num)
            tags = []
            if str(int(word_num)+1) in end_clusters:
                tags = [x + ')' for x in end_clusters.pop(str(int(word_num)+1))]
            if key in new_mentions:
                start_tags = []
                mentions = sorted(new_mentions[key], key=lambda x: int(x[1]), reverse=True)
                for cluster_id, end_i in mentions:
                    if int(end_i) == int(word_num) + 1:
                        start_tags.append('('+cluster_id+')')
                    else:
                        start_tags.append('('+cluster_id)
                        # LIFO, stack
                        end_clusters[end_i].append(cluster_id)
                tags = start_tags + tags
            if len(tags) > 0:
                if set(tags) != set(line[-1].split('|')):
                    print line[:3], line[-1], tags
                line[-1] = '|'.join(tags)
            new_corefs_file.write('\t'.join(line) + '\n')
    old_corefs_data.close()
    new_corefs_file.close()


generate_conll_corefs_file(generate_new_mentions(new_coref_clusters))