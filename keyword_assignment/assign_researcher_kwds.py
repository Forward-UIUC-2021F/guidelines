"""
In main loop, takes 103 s / 300 author.
Estimate time for 16,324 authors: < 2 hrs
"""
import math
import mysql.connector
from sklearn.cluster import DBSCAN

import numpy as np
import numpy.linalg as la

from utils import read_pickle_file, write_pickle_data, get_top_k, concat_paper_info, standardize_non_ascii

def normalize_embs(emb_arr):
    emb_norms = la.norm(emb_arr, axis=1)
    return emb_arr / emb_norms[:,None]

def normalize_vec(vec):
    return vec / la.norm(vec)


print("Loading and preprocessing data.")
data_root_dir = 'setup_data/'
keyword_embeddings_file = data_root_dir + "springer_keyword_embs.pickle"

keyword_embeddings = read_pickle_file(keyword_embeddings_file)
keyword_embeddings = normalize_embs(keyword_embeddings)


mydb = mysql.connector.connect(
  host="localhost",
  user="aukey2",
  password="aUkeyWwords",
  database="aukey2_experts_v2"
)
mycursor = mydb.cursor()

exp_multiplier = 7
exp_base = math.exp(exp_multiplier)
print("Hyper param multiplier: " + str(exp_multiplier))

def finger_print_author(query_author_id):
    """
    Assigns a set of keywords to an author.

    Arguments:
    - query_author_id: id of author for whom keywords are being assigned

    Returns: None. Inserts author-keyword assignments into database.

    The process transforms publication-keyword assigned scores using
    softmax-like-function f:
        score_i = a^(score_i) / sum_all_i { a^(score_i) }
        where a = 7

    and sums over each of these scores for each keyword across all
    the publications of an author.
    """

    # SQL statement implementing above ranking equation
    author_keywords_sql = """
        SELECT word_scores.id, FoS_name,
        SUM(citation * (score / sum_score)) AS word_score FROM

        (
            SELECT publication_id, FoS.id AS id, FoS_name,
            POWER(%s, Publication_FoS.score) AS score

            FROM Publication_FoS

            JOIN Publication_Author ON publication_mag_id = publication_id
            JOIN FoS ON FoS.id = Publication_FoS.FoS_id

            WHERE author_id = %s
        ) AS word_scores

        JOIN

        (
            SELECT citation, year, publication_id,
            SUM(POWER(%s, Publication_FoS.score)) AS sum_score

            FROM Publication_FoS

            JOIN Publication_Author ON publication_id = publication_mag_id
            JOIN Publication on publication_id = Publication.id

            WHERE author_id = %s
            GROUP BY publication_id
        ) AS publ_scores

        ON word_scores.publication_id = publ_scores.publication_id

        GROUP BY word_scores.id
        ORDER BY word_score DESC LIMIT 70
    """

    query_tuple = 2 * (str(exp_base), str(query_author_id))

    mycursor.execute(author_keywords_sql, query_tuple)
    selected_keyword_ts = mycursor.fetchall()
    num_candidates = len(selected_keyword_ts)

    # print(selected_keyword_ts)

    if num_candidates == 0:
        return

    selected_keyword_ids = [t[0] for t in selected_keyword_ts]
    selected_keyword_embs = keyword_embeddings[selected_keyword_ids]

    # Remove duplicates using clustering algorithm
    db = DBSCAN(eps=0.47815, min_samples=2).fit(selected_keyword_embs)
    labels = db.labels_


    max_candidates = 40
    group_to_score = {}
    ungrouped_words = []
    for i in range(num_candidates):

        num_total_selected = len(group_to_score) + len(ungrouped_words)
        if num_total_selected >= max_candidates:
            break

        kw_t = selected_keyword_ts[i]
        group_idx = labels[i]

        if group_idx == -1:
            ungrouped_words.append(kw_t)
        else:
            if group_idx not in group_to_score:
                group_to_score[group_idx] = [kw_t[0], kw_t[1], kw_t[2]]
            else:
                group_to_score[group_idx][2] += kw_t[2]

    grouped_keyword_scores = list(group_to_score.values()) + ungrouped_words

    # print(grouped_keyword_scores)
    max_keywords = 17
    top_keyword_ts = get_top_k(grouped_keyword_scores, min(max_keywords, len(grouped_keyword_scores)), lambda t: t[2])


    # Insert assignments into db
    for kw_t in top_keyword_ts:
      insert_sql = "INSERT INTO Author_FoS (author_id, FoS_id, score) VALUES (%s, %s, %s)"
      insert_values = (str(query_author_id), str(kw_t[0]), str(kw_t[2]))
      mycursor.execute(insert_sql, insert_values)


print("Generating author fingerprint(s)")

mycursor.execute("SELECT id FROM Author")
authors = mycursor.fetchall()

au_i = 0
for author_t in authors:
    author_id = author_t[0]

    try:
        finger_print_author(author_id)
        mydb.commit()
    except:
        print("Error for " + str(author_id))

    if au_i % 300 == 0:
        print("On " + str(au_i) + "th author")

    au_i += 1

mydb.commit()
