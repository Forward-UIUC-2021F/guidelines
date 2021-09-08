from utils import gen_sql_in_tup, return_print_err, copy_temporary_table, drop_view, drop_table
import mysql.connector


def store_keywords(keyword_ids, cur, make_copy=True):
    """
    Stores top 10 similar keywords for each input keyword

    Arguments:
    - keyword_ids: list of ids of input keywords
    - cur: db cursor

    Returns: None. each entry in Top_Keywords table is of the form (parent_id, keyword_id, npmi).
    - parent_id: id of the original input keyword
    - keyword_id: id of similar keyword
    - npmi is a similarity score between the two keywords

    Note: the identity row for each keyword_id is included by default with
    similarity score 1 (i.e. for each kw_id in keywords_ids, there will be a
    row in Top_Keywords of (kw_id, kw_id, 1))
    """
    fields_in_sql = gen_sql_in_tup(len(keyword_ids))

    drop_table(cur, "Top_Keywords")
    get_related_keywords_sql = """
        CREATE TABLE Top_Keywords (
            parent_id INT,
            id INT,
            npmi DOUBLE,
            PRIMARY KEY(parent_id, id)
        )

        SELECT parent_id, id, npmi
        FROM
        (
            SELECT parent_id, id, npmi,
            @kw_rank := IF(@current_parent = parent_id, @kw_rank + 1, 1) AS kw_rank,
            @current_parent := parent_id
            FROM
            (
                (SELECT id2 AS parent_id,
                id1 AS id, npmi
                FROM FoS_npmi_Springer
                WHERE id2 IN """ + fields_in_sql + """)

                UNION

                (SELECT
                id1 AS parent_id,
                id2 as id, npmi
                FROM FoS_npmi_Springer
                WHERE id1 IN """ + fields_in_sql + """)
            ) as top_keywords
            ORDER BY parent_id, npmi DESC
        ) AS ranked_keywords

        WHERE kw_rank <= 10
    """
    get_related_query_params = 2 * keyword_ids
    cur.execute(get_related_keywords_sql, get_related_query_params)


    append_given_sql = """
        INSERT INTO Top_Keywords
        (parent_id, id, npmi)
        VALUES
        """ + ",\n".join(["(%s, %s, 1)"] * len(keyword_ids))

    append_given_query_params = [id for id in keyword_ids for i in range(2)]

    cur.execute(append_given_sql, append_given_query_params)




def compute_author_keyword_ranks(cur):
    """
    Computes and stores score for each publication

    Arguments:
    - cur: db cursor

    Returns: None


    Each publication has an associated score for each input keyword.
    The score between an input keyword and a paper is computed by determining if there is any match between the top ten similar keywords for the input keyword and the paper's keyword assignments (see assign_paper_kwds.py for details on  how keywords are assigned to papers).

    The maximum scoring match is picked and the final score for an input
    keyword is computed as max_npmi * citation. A score is computed for each
    publication-keyword pair.
    """

    drop_table(cur, "Author_Keyword_Scores")
    create_author_ranks_sql = """
        CREATE TEMPORARY TABLE Author_Keyword_Scores (
            author_id BIGINT,
            parent_id INT,
            kw_id BIGINT,
            citation INT,
            publication_id BIGINT,
            comp_score DECIMAL,
            PRIMARY KEY(author_id, publication_id, parent_id)
        )

        SELECT author_id,
        parent_id,
        MIN(FoS_id) AS kw_id,
        MAX(max_npmi) AS max_npmi,
        citation,
        Publication_Scores.publication_id,
        MAX(max_npmi) * citation AS comp_score

        FROM
        (
            SELECT author_id, publication_id,
            MAX(npmi) as max_npmi,
            IFNULL(citation, 0) AS citation

            FROM Top_Keywords
            JOIN Publication_FoS ON Top_Keywords.id = FoS_id
            JOIN Publication_Author ON publication_mag_id = publication_id
            JOIN Publication ON publication_id = Publication.id
            GROUP BY author_id, Publication.id, parent_id
        ) AS Publication_Scores

        JOIN

        (
            SELECT publication_id,
            parent_id, FoS_id, npmi

            FROM Top_Keywords
            JOIN Publication_FoS ON FoS_id = id
        ) AS Publication_Top_Keywords

        ON ABS(max_npmi - npmi) < 0.000001
        AND Publication_Top_Keywords.publication_id = Publication_Scores.publication_id

        GROUP BY author_id, Publication_Scores.publication_id, parent_id
    """
    cur.execute(create_author_ranks_sql)

    # copy_temporary_table(cur, "Author_Keyword_Scores")




def rank_authors_keyword(keyword_ids, cur):
    """
    Main function that returns the top ranked authors for some keywords

    Arguments:
    - keyword_ids: list of keyword ids by which we must rank
    - cur: db cursor

    Returns: list of python dicts each representing an author.
    Each dict has keys 'name', 'id', and 'score' of author. During ranking
    each keyword is weighted separately and equally.
    """

    # Store top similar keywords
    store_keywords(keyword_ids, cur)

    # Compute scores between each publication and input keyword
    compute_author_keyword_ranks(cur, rank_type, entity_type)


    # Aggregate scores for each author
    get_author_ranks_sql = """
        SELECT Author.id, Author.name,
        SUM(comp_score) AS score

        FROM Author_Keyword_Scores
        JOIN Author ON id = author_id

        GROUP BY author_id
        ORDER BY score DESC
        LIMIT 15
    """
    cur.execute(get_author_ranks_sql)
    author_ranks = cur.fetchall()

    res = [{
        'id': t[0],
        'name': t[1],
        'score': t[2]
    } for t in author_ranks]

    top_author_ids = [t["id"] for t in res]

    author_id_to_idx = {}
    for i in range(len(top_author_ids)):
        author_id = top_author_ids[i]
        author_id_to_idx[author_id] = i

    cur.close()
    return res




if __name__ == '__main__':

    # Setting up db
    db = mysql.connector.connect(
      host="localhost",
      user="root",
      password="<replace with your db password>",
      database="<replace with your db name>"
    )
    cur = db.cursor()

    # Ids of all keywords can be found in FoS table
    # Corresponds to keywords 'data mining' and 'security'
    test_kwd_ids = [4, 9]

    top_authors = rank_authors_keyword(test_kwd_ids, cur)
    print(top_authors)
