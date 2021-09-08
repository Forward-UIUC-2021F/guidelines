from sentence_transformers import SentenceTransformer
import numpy as np
import numpy.linalg as la


def cosine_sim(vec1, vec2):
    return np.dot(vec1, vec2) / (la.norm(vec1) * la.norm(vec2))

model = SentenceTransformer('bert-base-nli-mean-tokens')

abstract = 'Prediction tasks over nodes and edges in networks require careful effort in engineering features used by learning algorithms. Recent research in the broader field of representation learning has led to significant progress in automating prediction by learning the features themselves. However, present feature learning approaches are not expressive enough to capture the diversity of connectivity patterns observed in networks. Here we propose node2vec, an algorithmic framework for learning continuous feature representations for nodes in networks. In node2vec, we learn a mapping of nodes to a low-dimensional space of features that maximizes the likelihood of preserving network neighborhoods of nodes. We define a flexible notion of a node\'s network neighborhood and design a biased random walk procedure, which efficiently explores diverse neighborhoods. Our algorithm generalizes prior work which is based on rigid notions of network neighborhoods, and we argue that the added flexibility in exploring neighborhoods is the key to learning richer representations. We demonstrate the efficacy of node2vec over existing state-of-the-art techniques on multi-label classification and link prediction in several real-world networks from diverse domains. Taken together, our work represents a new way for efficiently learning state-of-the-art task-independent representations in complex networks.'
candidate_keywords = ['natural language processing', 'word embedding', 'embedding', 'happy', 'sad', 'chemistry', 'computer science', 'graphs', 'hierarchy']

sentences = [abstract] + candidate_keywords
sentence_embeddings = model.encode(sentences)


print("Keyword scores for")
print(abstract + "\n")

for kw_i in range(1, len(sentences)):
    sim_score = cosine_sim(sentence_embeddings[0], sentence_embeddings[kw_i])
    print("\tScore for " + sentences[kw_i] + ": " + str(sim_score))
