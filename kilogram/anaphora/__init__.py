from __future__ import division
from gensim.models import word2vec
from numpy import exp, dot, zeros, outer, random, dtype, float32 as REAL,\
    uint32, seterr, array, uint8, vstack, argsort, fromstring, sqrt, newaxis,\
    ndarray, empty, sum as np_sum, prod
from gensim import utils, matutils
w2v_model = word2vec.Word2Vec.load('/Users/dragoon/Downloads/300features_40minwords_10context')
w2v_model.init_sims()

from collections import defaultdict


class CorefCluster(object):
    # groups of mentions, can be headword or some other grouping
    mention_groups = None
    non_noun_lemmas = None
    coref_cluster_id = None

    def __init__(self):
        self.non_noun_lemmas = set()
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
            self.non_noun_lemmas.add(mention.head_lemma)
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

    def __eq__(self, mention):
        return self.head_lemma == mention.head_lemma


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
    def get_feature_vector(gold_head_lemmas, test_head_lemma):
        try:
            test_vector = w2v_model.syn0norm[w2v_model.vocab[test_head_lemma].index]
        except KeyError:
            raise NoWordInVocabularyError()
        gold_words = [x for x in gold_head_lemmas if x in w2v_model.vocab]
        if len(gold_words) == 0:
            raise EmptyGoldCluster()
        vectors = vstack(w2v_model.syn0norm[w2v_model.vocab[word].index] for word in gold_words).astype(REAL)
        mean = matutils.unitvec(vectors.mean(axis=0)).astype(REAL)
        similarity = dot(test_vector, mean)
        min_sim = min([dot(matutils.unitvec(x), test_vector) for x in vectors])
        max_sim = max([dot(matutils.unitvec(x), test_vector) for x in vectors])
        feature_vector = [similarity, min_sim, max_sim]
        #feature_vector.extend(list(test_vector))
        #feature_vector.extend(list(mean))
        return feature_vector

    def get_features(self):
        feature_vectors = []
        labels = []
        for doc_coref_clusters in self.coref_clusters.values():
            for coref_cluster in doc_coref_clusters.values():
                local_fv, local_lab = self._get_features_for_cluster(coref_cluster)
                feature_vectors.extend(local_fv)
                labels.extend(local_lab)
        print 'Skipped Small Cluster:', self.skipped_empty_cluster
        print 'Skipped Empty Gold Cluster:', self.skipped_empty_gold
        print 'Skipped No Test Word Vector:', self.skipped_no_word
        print 'Passed:', self.passed
        return feature_vectors, labels

    def _get_features_for_cluster(self, coref_cluster):
        feature_vectors = []
        labels = []
        gold_clusters = defaultdict(list)
        # populate gold clusters
        for head_lemma, mention_group in coref_cluster.mention_groups.items():
            for mention in mention_group.mentions:
                gold_clusters[mention.gold_coref_id].append(head_lemma)
        # generate features
        for head_lemma, mention_group in coref_cluster.mention_groups.items():
            mention_gold_ids = [mention.gold_coref_id for mention in mention_group.mentions]
            for gold_cluster_id, gold_lemmas in gold_clusters.items():
                label = int(gold_cluster_id in mention_gold_ids)
                if label == 1:
                    # exclude mention from gold_values if same cluster
                    gold_lemmas = [x for x in gold_lemmas if x != head_lemma]
                if len(gold_lemmas) == 0:
                    self.skipped_empty_cluster += 1
                    continue
                try:
                    feature_vector = FeatureExtractor.get_feature_vector(gold_lemmas, head_lemma)
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


gold_corefs_data = parse_corefs_data('/Users/dragoon/Downloads/corefs_gold_mentions.txt')
feature_vectors, labels = FeatureExtractor(gold_corefs_data).get_features()


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
        mention_group_lemmas = mention_groups.keys()
        for i, mention_group_lemma in enumerate(mention_group_lemmas):
            other_mention_lemmas = mention_group_lemmas[:]
            del other_mention_lemmas[i]
            try:
                features = FeatureExtractor.get_feature_vector(other_mention_lemmas, mention_group_lemma)
            except:
                self.skipped += 1
                continue
            class1, class2 = self.clf.predict_proba(features)[0]
            #print mention.gold_coref_id, class1, class2
            mention_group_classes.append((class1, mention_groups[mention_group_lemma]))
        mention_group_classes.sort(key=lambda x: x[0])
        return mention_group_classes

    def evaluate_internal(self):
        for key, doc_clusters in self.coref_clusters.iteritems():
            doc_id, par_id = key
            for auto_cluster_id, coref_cluster in doc_clusters.iteritems():
                mention_groups = coref_cluster.mention_groups
                if len(mention_groups) < 2:
                    continue
                mention_classes = self._get_mention_classes(mention_groups)
                if not mention_classes:
                    continue
                lowest = mention_classes.pop(-1)
                other_gold_ids = set([y.gold_coref_id for prob, mention_group in mention_classes
                                      for y in mention_group.mentions])
                lowest_gold_ids = set([mention.gold_coref_id for mention in lowest[1].mentions])
                if lowest[0] > 0.5:
                    # need to re-assign at least one entity
                    self.internal_precision.append(int(not lowest_gold_ids.intersection(other_gold_ids)))
                    self.internal_recall += 1
                if len(other_gold_ids.union(lowest_gold_ids)) > 1:
                    self.internal_recall += 1
        print 'Total identified cases:', len(self.internal_precision)
        print 'Skipped:', self.skipped
        print 'Precision:', sum(self.internal_precision)/len(self.internal_precision)
        print 'Recall: ', sum(self.internal_precision)/self.internal_recall
        # reset
        self.skipped = 0
        self.internal_recall = 0
        self.internal_precision = []

    def generate_external_file(self):
        import copy
        copy_coref_clusters = copy.deepcopy(self.coref_clusters)
        for key, doc_clusters in copy_coref_clusters.iteritems():
            doc_id, par_id = key
            non_matching_groups = []
            while True:
                for auto_cluster_id, coref_cluster in doc_clusters.iteritems():
                    non_matching = self._doesnt_match(coref_cluster)
                    if non_matching:
                        del coref_cluster.mention_groups[non_matching.head_lemma]
                        non_matching_groups.append(non_matching)
                if len(non_matching_groups) == 0:
                    break
                while len(non_matching_groups)>0:
                    mention_group = non_matching_groups.pop()
                    # try to assign group to a new cluster
                    matched = False
                    for coref_cluster in doc_clusters.values():
                        coref_cluster_lemmas = coref_cluster.mention_groups.keys()
                        try:
                            features = FeatureExtractor.get_feature_vector(coref_cluster_lemmas, mention_group.head_lemma)
                        except:
                            continue
                        if self.clf.predict(features)[0] == 1:
                            # add to the current cluster
                            coref_cluster.add_mention_group(mention_group)
                            matched = True
                            break
                    if not matched:
                        # create new cluster
                        new_cluster_id = max([int(x) for x in doc_clusters.keys()]) + 1
                        doc_clusters[new_cluster_id].coref_cluster_id = new_cluster_id
                        doc_clusters[new_cluster_id].add_mention_group(mention_group)
        return copy_coref_clusters

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.linear_model import *
from sklearn.cross_validation import cross_val_score

clf_extra_trees = ExtraTreesClassifier(n_estimators=50, class_weight='auto', bootstrap=True)
clf_log_reg = LogisticRegression(class_weight='auto')

# QUICK EVALUATION
print cross_val_score(clf_extra_trees, feature_vectors, labels, cv=5)
print cross_val_score(clf_log_reg, feature_vectors, labels, cv=5)

# Fit before evaluation
clf_extra_trees.fit(feature_vectors, labels)

real_coref_data = parse_corefs_data('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs.txt')
reassigner = ClusterReassigner(gold_corefs_data, clf_extra_trees)
reassigner.evaluate_internal()

reassigner.coref_clusters = real_coref_data
new_coref_clusters = reassigner.generate_external_file()

new_mentions = defaultdict(dict)
old_mentions = defaultdict(dict)
for doc_id, doc_coref_clusters in new_coref_clusters.iteritems():
    doc_id, par_id = doc_id
    for cluster_id, coref_cluster in doc_coref_clusters.iteritems():
        for mention_group in coref_cluster.mention_groups.itervalues():
            for mention in mention_group.mentions:
                new_mentions[(doc_id, par_id, int(mention.sent_id), mention.start_i)][mention.end_i] = cluster_id
for doc_id, doc_coref_clusters in real_coref_data.iteritems():
    doc_id, par_id = doc_id
    for cluster_id, coref_cluster in doc_coref_clusters.iteritems():
        for mention_group in coref_cluster.mention_groups.itervalues():
            for mention in mention_group.mentions:
                old_mentions[(doc_id, par_id, int(mention.sent_id), mention.start_i)][mention.end_i] = cluster_id
diff_dict = {}
for key, end_cluster_dict in new_mentions.items():
    old_end_cluster_dict = old_mentions[key]
    # find non-matching clusters
    non_matching = []
    for end_i, cluster_id in end_cluster_dict.items():
        if old_end_cluster_dict[end_i] != cluster_id:
            non_matching.append()
    diff_dict[key[:-1]] = {'cluster_id': cluster_id, 'old_cluster_id': old_mentions[key], 'end_i': key[-1]}

print 'Differences length:', len(diff_dict)


def generate_conll_corefs_file(diff_dict):
    old_corefs_data = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conlloutput-Sun-Apr-19-15-07-03-CEST-2015.coref.predicted.txt')
    new_corefs_data = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conlloutput-Sun-Apr-19-15-07-03-CEST-2015.coref.predicted.new.txt', 'w')

    sent_id = 0
    replacements = defaultdict(list)
    for line_num, line in enumerate(old_corefs_data):
        line = line.strip()
        if line.startswith(('#begin', '#end')):
            sent_id = 0
            new_corefs_data.write(line+'\n')
        elif len(line) == 0:
            sent_id += 1
            new_corefs_data.write(line+'\n')
        else:
            line = line.split('\t')
            cluster_tag = line[-1]
            doc_id, par_id, word_num = line[:3]
            key = (doc_id, par_id, sent_id, word_num)
            if line_num in replacements:
                replacement = replacements.pop(line_num)
                for old_cluster_id, new_cluster_id in replacement:
                    cluster_tag = cluster_tag.replace(old_cluster_id, new_cluster_id)
                line[-1] = cluster_tag
            if key in diff_dict:
                current_span = diff_dict[key]
                #print line, diff_dict[key]
                if cluster_tag.count(current_span['old_cluster_id']) == 1:
                    cluster_tag = cluster_tag.replace(current_span['old_cluster_id'], current_span['cluster_id'])
                    line[-1] = cluster_tag
                    if int(word_num) + 1 < int(current_span['end_i']):
                        # remember to replace
                        replacements[line_num+int(current_span['end_i']) - int(word_num) - 1].append((current_span['old_cluster_id'], current_span['cluster_id']))
                else:
                    print 'LOL'
            new_corefs_data.write('\t'.join(line) + '\n')

    old_corefs_data.close()
    new_corefs_data.close()


generate_conll_corefs_file(diff_dict)