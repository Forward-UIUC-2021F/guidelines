import sys
import torch
import pickle
import mysql.connector
from sentence_transformers import SentenceTransformer

sys.path.insert(1, '../utils')
from utils import read_pickle_file, write_pickle_data, concat_paper_info


db = mysql.connector.connect(
  host="localhost",
  user="<user>",
  password="<db password>",
  database="<db>"
)
cursor = db.cursor()


emb_out_file = "SB_paper_embeddings.pickle"
idx_out_file = "SB_paper_embedding_mag_ids.pickle"


cursor.execute("""
    SELECT id, title, abstract
    FROM Publication
    WHERE abstract IS NOT NULL
""")
paper_ts = cursor.fetchall()


print("Loading and preprocessing data/models")
model = SentenceTransformer('bert-base-nli-mean-tokens')
# model = SentenceTransformer('bert-base-nli-mean-tokens')
# model = BertForMaskedLM.from_pretrained('sentence-transformers/paraphrase-MiniLM-L6-v2')

mag_ids = [t[0] for t in paper_ts]
paper_raw = [concat_paper_info(t[1], t[2]) for t in paper_ts]


print("Getting embeddings for " + str(len(paper_raw)) + " papers")
# paper_embeddings = run_model(paper_raw)
paper_embeddings = model.encode(paper_raw, show_progress_bar=True, device='cuda')


print("Done. Saving data")
write_pickle_data(paper_embeddings, emb_out_file)
write_pickle_data(mag_ids, idx_out_file)
