import numpy as np
import numpy.linalg as la

from sklearn.cluster import DBSCAN
from sentence_transformers import SentenceTransformer

def normalize_embs(emb_arr):
    emb_norms = la.norm(emb_arr, axis=1)
    return emb_arr / emb_norms[:,None]


model = SentenceTransformer('bert-base-nli-mean-tokens')


def remove_duplicates(keywords):

    keyword_embs = model.encode(keywords)
    keyword_embs = np.array(keyword_embs)
    keyword_embs = normalize_embs(keyword_embs)

    db = DBSCAN(eps=0.47815, min_samples=2).fit(keyword_embs)


    # Contains group index for each keyword
    # See sklearn documentation of DBSCAN for more info
    labels = db.labels_


    # Removing duplicates based on keyword groupings
    picked_groups = set()
    unique_keywords = []

    for i in range(len(keywords)):
        keyword = keywords[i]
        group_idx = labels[i]

        if group_idx == -1:
            unique_keywords.append(keyword)
        elif group_idx not in picked_groups:
            picked_groups.add(group_idx)
            unique_keywords.append(keyword)

    return unique_keywords


test_keywords = ['algorithm', 'algorithms', 'machines']
print(remove_duplicates(test_keywords))
