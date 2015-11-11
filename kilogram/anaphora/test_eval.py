from collections import defaultdict
import itertools

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

    def add_non_noun_group(self, mention_group):
        # change mention cluster_ids
        for mention in mention_group.mentions:
            mention.coref_cluster_id = self.coref_cluster_id
        if mention_group.head_lemma in self.non_noun_groups:
            self.non_noun_groups[mention_group.head_lemma].mentions.extend(mention_group.mentions)
        else:
            self.non_noun_groups[mention_group.head_lemma] = mention_group

    def add_mention(self, mention):
        if mention.head_pos not in ('NN', 'NNS', 'NNP') \
            or mention.mention.lower() in ('my', 'mine', 'i', 'he', 'theirs', 'you', 'itself') \
                or mention.head_lemma_orig.isupper():
            self.non_noun_groups[mention.head_lemma].add_mention(mention)
        else:
            self.mention_groups[mention.head_lemma].add_mention(mention)
        self.coref_cluster_id = mention.coref_cluster_id

    def add_cluster(self, to_merge_cluster):
        for mention_group in to_merge_cluster.mention_groups.values():
            self.add_mention_group(mention_group)
        for non_noun_group in to_merge_cluster.non_noun_groups.values():
            self.add_non_noun_group(non_noun_group)

    def __len__(self):
        return len(self.mention_groups)

    def is_and(self):
        for mention_group in self.mention_groups.values():
            for mention in mention_group.mentions:
                if ' and ' in mention.mention:
                    return True
        return False


class MentionGroup(object):
    mentions = None
    head_lemma = None

    def __init__(self):
        self.mentions = []

    def __repr__(self):
        return str(self.mentions)

    def add_mention(self, mention):
        self.mentions.append(mention)
        self.head_lemma = mention.head_lemma

    @property
    def entity_type(self):
        try:
            return [m.entity_type for m in self.mentions if m.entity_type][0]
        except:
            return None

    @property
    def entity_url(self):
        try:
            return [m.entity_url for m in self.mentions if m.entity_url][0]
        except:
            return None


class Mention(object):
    mention = None
    sent_id = None
    mention_id = None
    start_i = None
    end_i = None
    head_lemma = None
    head_pos = None
    ner_type = None
    ner_tag = None
    pos_seq = None
    coref_cluster_id = None
    gold_coref_id = None
    entity_url = None
    entity_type = None

    def __init__(self, date_tuple):
        self.sent_id = int(date_tuple[2])
        self.mention_id = date_tuple[3]
        self.start_i = int(date_tuple[4])
        self.end_i = int(date_tuple[5])
        self.mention = date_tuple[6]
        self.ner_entity = date_tuple[7]
        self.ner_tag = date_tuple[12]
        self.head_lemma_orig = date_tuple[8]
        self.head_lemma = self.mention.lower()
        self.head_pos = date_tuple[9]
        self.coref_cluster_id = date_tuple[10]
        self.gold_coref_id = date_tuple[11]
        self.pos_seq = date_tuple[13]
        if type(date_tuple[14]) == list:
            # SPOTLIGHT
            if len(date_tuple[14]) > 0:
                self.entity_url = self.entity_type = date_tuple[14][0]
        elif date_tuple[14] != 'null':
            self.entity_url = date_tuple[14]
            try:
                self.entity_type = ['<dbpedia:' + x.rsplit('/', 1)[1] + '>' for x in date_tuple[15].split()][0]
            except:
                pass

    def __unicode__(self):
        return self.mention

    def __repr__(self):
        return self.mention


def parse_corefs_data_spot(filename, spot_filename):
    """
    :param filename:
    :param entity_filename: - spotlighted filename
    :return:
    """
    spotlight_dict = {}
    entity_data = open(spot_filename).readlines()
    entity_data = [x.strip().split('\t') for x in entity_data if x.strip() and not x.startswith(('#begin', '#end'))]
    entity_data = [x for x in entity_data if x[-1] != '-']
    for doc_id, par_id, sent_id, start_i, word, link in entity_data:
        spotlight_dict[(doc_id, par_id, sent_id, start_i)] = link[1:-1].split('|')

    corefs_data = open(filename).readlines()
    corefs_data = [x.strip().split('\t') for x in corefs_data]
    # remove header
    del corefs_data[0]

    i = 0
    for line in corefs_data:
        start_i = int(line[4])
        end_i = int(line[5])
        head_lemma = line[8]
        spot_entities = set()
        for index in range(start_i, end_i):
            key = tuple(line[:3] + [str(index)])
            if key in spotlight_dict:
                entity_url, orig_text = spotlight_dict[key]
                if head_lemma.lower() in entity_url.lower():
                    spot_entities.add(entity_url)
        if spot_entities:
            i += 1
        line.append(list(spot_entities))
    print 'Total spotlighted:', i

    # dict of meta information about auto cluster,
    coref_clusters = defaultdict(lambda: defaultdict(lambda: CorefCluster()))
    for data_tuple in corefs_data:
        coref_clusters[(data_tuple[0], data_tuple[1])][data_tuple[10]].add_mention(Mention(data_tuple))
    return coref_clusters


def parse_corefs_data(filename, entity_filename):
    corefs_data = open(filename).readlines()
    entity_data = open(entity_filename).readlines()
    corefs_data = [x.strip() + '\t' + '\t'.join(y.strip().split('\t')[14:]) for x,y in zip(corefs_data, entity_data)]
    corefs_data = [x.strip().split('\t') for x in corefs_data]
    # remove header
    del corefs_data[0]

    # dict of meta information about auto cluster,
    coref_clusters = defaultdict(lambda: defaultdict(lambda: CorefCluster()))
    for data_tuple in corefs_data:
        coref_clusters[(data_tuple[0], data_tuple[1])][data_tuple[10]].add_mention(Mention(data_tuple))

    i = 0
    for line in corefs_data:
        if line[14] != 'null':
            i += 1
    print 'Total annotated:', i
    return coref_clusters

gold_corefs_data = parse_corefs_data('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-test.txt',
                                     '/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-test_annotated_single_entity_col6.txt')

#gold_corefs_data = parse_corefs_data_spot('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/corefs-test.txt',
#                                          '/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conll-test.predicted.txt.spot05_200_handcorrected')


class Evaluator(object):
    TP = 0
    TN = 0
    FN = 0
    FP = 0

    def __init__(self):
        self.TP = 0
        self.FN = 0
        self.TN = 0
        self.FP = 0

splitEvaluator = Evaluator()
mergeEvaluator = Evaluator()
orig_split_evaluator = Evaluator()
orig_merge_evaluator = Evaluator()


total_groups = 0

def generate_external_file(coref_clusters):
    import copy
    copy_coref_clusters = copy.deepcopy(coref_clusters)
    for key, doc_clusters in copy_coref_clusters.iteritems():
        non_matching_clusters = []
        cluster_entity_mapping = defaultdict(set)
        for coref_cluster in doc_clusters.itervalues():

            for mention_group in coref_cluster.mention_groups.values():
                for mention in mention_group.mentions:
                    if mention.entity_url is not None and not coref_cluster.is_and():
                        cluster_entity_mapping[mention.entity_url].add(coref_cluster.coref_cluster_id)

            if len(coref_cluster.non_noun_groups) > 0:
                continue

            non_matching_groups = doesnt_match(coref_cluster)
            if non_matching_groups:
                for non_matching_group in non_matching_groups:
                    del coref_cluster.mention_groups[non_matching_group.head_lemma]
                non_matching_clusters.append(non_matching_groups)

        global total_groups
        total_groups += len(non_matching_clusters)
        while len(non_matching_clusters) > 0:
            mention_groups = non_matching_clusters.pop()
            new_cluster_id = min(y.mention_id for x in mention_groups for y in x.mentions)
            doc_clusters[new_cluster_id].coref_cluster_id = new_cluster_id
            for mention_group in mention_groups:
                doc_clusters[new_cluster_id].add_mention_group(mention_group)

        for entity_url, cluster_ids in cluster_entity_mapping.iteritems():
            if len(cluster_ids) > 1:
                # START: EVALUATE: ORIG
                combinations = list(itertools.combinations([(mention.gold_coref_id, clust_id) for
                                clust_id in cluster_ids for mention_group in doc_clusters[clust_id].mention_groups.values()
                                for mention in mention_group.mentions if mention.gold_coref_id != '-1'], 2))
                evaluate(combinations, orig_merge_evaluator)
                # END: EVALUATE: ORIG

                min_cluster = min(cluster_ids, key=lambda x: int(x))
                cluster_ids.remove(min_cluster)
                for cluster_id in cluster_ids:
                    try:
                        cand_merge_cluster = doc_clusters.pop(cluster_id)
                    except:
                        continue
                    doc_clusters[min_cluster].add_cluster(cand_merge_cluster)

                # START: EVALUATE
                combinations = list(itertools.combinations([(mention.gold_coref_id, min_cluster) for
                                mention_group in doc_clusters[min_cluster].mention_groups.values()
                                for mention in mention_group.mentions if mention.gold_coref_id != '-1'], 2))
                evaluate(combinations, mergeEvaluator)
                # END: EVALUATE

    return copy_coref_clusters


def doesnt_match(doc_cluster):
    entity_types = set()
    entity_words = {'the', 'a'}
    all_mentions = [mention for mention_group in doc_cluster.mention_groups.values() for mention in mention_group.mentions]
    for mention in sorted(all_mentions, key=lambda x: (x.sent_id, x.start_i)):
        if mention.entity_url is not None:
            cur_entity_words = set(mention.ner_entity.lower().split())
            if not entity_words.issuperset(cur_entity_words):
                entity_words = entity_words.union(cur_entity_words)
                entity_types.add(mention.entity_url)
            if cur_entity_words.issuperset(entity_words):
                entity_types = {mention.entity_url}
                entity_words = cur_entity_words
    # START: ORIG EVALUATION
    if len(set([mention.entity_url for mention in all_mentions if mention.entity_url])) > 1:
        combinations = list(itertools.combinations([(mention.gold_coref_id, 0) for mention in all_mentions if mention.gold_coref_id != '-1'], 2))
        evaluate(combinations, orig_split_evaluator)
    # END: ORIG EVALUATION
    if len(entity_types) > 1:
        latest_id = (0, 0)
        latest_group = None
        for mention_group in doc_cluster.mention_groups.values():
            for mention in mention_group.mentions:
                if mention.entity_url in entity_types:
                    if latest_id < (mention.sent_id, mention.start_i):
                        latest_group = mention_group
                        latest_id = (mention.sent_id, mention.start_i)
        # collection other mention_groups that share exclusive words with the latest group
        exclusive_words = set(latest_group.head_lemma.split())
        for mention_group in doc_cluster.mention_groups.values():
            if mention_group.entity_url and mention_group.entity_url != latest_group.entity_url:
                exclusive_words = exclusive_words.difference(mention_group.head_lemma.split())

        non_matching_groups = []
        for mention_group in doc_cluster.mention_groups.values():
            if (mention_group.entity_url is None and exclusive_words.intersection(mention_group.head_lemma.split()))\
                    or mention_group.entity_url == latest_group.entity_url:
                non_matching_groups.append(mention_group)

        # START: EVALUATION
        non_matching_ids = []
        other_ids = []
        for mention_group in doc_cluster.mention_groups.values():
            if (mention_group.entity_url is None and exclusive_words.intersection(mention_group.head_lemma.split()))\
                    or mention_group.entity_url == latest_group.entity_url:
                for mention in mention_group.mentions:
                    if mention.gold_coref_id != '-1':
                        non_matching_ids.append((mention.gold_coref_id, 1))
            else:
                for mention in mention_group.mentions:
                    if mention.gold_coref_id != '-1':
                        other_ids.append((mention.gold_coref_id, 2))

        combinations = list(itertools.combinations(non_matching_ids+other_ids, 2))
        evaluate(combinations, splitEvaluator)

        # END: EVALUATION

        return non_matching_groups

    # START: EVALUATION
    if len(set([mention.entity_url for mention in all_mentions if mention.entity_url])) > 1:
        combinations = list(itertools.combinations([(mention.gold_coref_id, 0) for mention in all_mentions if mention.gold_coref_id != '-1'], 2))
        evaluate(combinations, splitEvaluator)
    # END: EVALUATION

    return None


def evaluate(combinations, evaluator):
    for elem1, elem2 in combinations:
        elem1_gold, elem1_system = elem1
        elem2_gold, elem2_system = elem2
        if elem1_gold == elem2_gold:
            if elem1_system == elem2_system:
                evaluator.TP += 1
            else:
                evaluator.FN += 1
        else:
            if elem1_system == elem2_system:
                evaluator.FP += 1
            else:
                evaluator.TN += 1


def generate_new_mentions(new_coref_clusters):
    new_mentions = defaultdict(list)
    for doc_id, doc_coref_clusters in new_coref_clusters.iteritems():
        doc_id, par_id = doc_id
        for cluster_id, coref_cluster in doc_coref_clusters.iteritems():
            for mention_group in coref_cluster.mention_groups.itervalues():
                for mention in mention_group.mentions:
                    key = (doc_id, par_id, mention.sent_id, mention.start_i)
                    new_mentions[key].append((cluster_id, mention.end_i))
            for mention_group in coref_cluster.non_noun_groups.itervalues():
                for mention in mention_group.mentions:
                    key = (doc_id, par_id, mention.sent_id, mention.start_i)
                    new_mentions[key].append((cluster_id, mention.end_i))
    return new_mentions


def generate_conll_corefs_file(new_mentions):
    old_corefs_data = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conll-test.predicted.txt')
    new_corefs_file = open('/Users/dragoon/Projects/stanford-corenlp-full-2015-01-30/CoreNLP/conll-test.predicted.new.txt', 'w')

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
            word_num = int(word_num)
            key = (doc_id, par_id, sent_id, word_num)
            tags = []
            if word_num+1 in end_clusters:
                tags = [x + ')' for x in end_clusters.pop(word_num+1)]
            if key in new_mentions:
                start_tags = []
                mentions = sorted(new_mentions[key], key=lambda x: int(x[1]), reverse=True)
                for cluster_id, end_i in mentions:
                    if end_i == word_num + 1:
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
            if len(tags) == 0 and line[-1] != '-':
                line[-1] = '-'
            new_corefs_file.write('\t'.join(line) + '\n')
    old_corefs_data.close()
    new_corefs_file.close()


new_coref_clusters = generate_external_file(gold_corefs_data)
print total_groups
#generate_conll_corefs_file(generate_new_mentions(new_coref_clusters))


print 'Original Split values: ', orig_split_evaluator.TP, orig_split_evaluator.FP, orig_split_evaluator.TN, orig_split_evaluator.FN
print 'Split values: ', splitEvaluator.TP, splitEvaluator.FP, splitEvaluator.TN, splitEvaluator.FN
print 'Merge values: ', mergeEvaluator.TP, mergeEvaluator.FP, mergeEvaluator.TN, mergeEvaluator.FN
print 'Original Merge values: ', orig_merge_evaluator.TP, orig_merge_evaluator.FP, orig_merge_evaluator.TN, orig_merge_evaluator.FN