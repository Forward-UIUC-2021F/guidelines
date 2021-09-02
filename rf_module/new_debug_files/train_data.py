from sklearn import datasets
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import CountVectorizer

import joblib
import csv

'''
Gets the full feature list to be used as a training dataset
    Parameters:
            path(string): path of the file to be used
    Returns:
        f: the list of features that would be used for training
        labels: the labels of what the paper was classified as
'''
def train(path):
    features = pd.read_csv(path)
    labels = np.array(features['label'])

    # drops column keyword and label 
    features = features.drop('keyword', axis = 1)
    features = features.drop('label', axis = 1)
    # use the title vector as a feature by converting it to a counter
    #title_counter = convert_to_counter(np.array(features['vector_t']))
    features = features.drop('vector_t', axis = 1)
    features = features.drop('vector_s', axis = 1)
    features = features.drop('position_k', axis = 1)
    features = features.drop('position_d', axis = 1)
    features = np.array(features)
    f = []
    # for i in range(len(features)):
    #     f.append(np.concatenate([features[i], title_counter[i]]))
    return features, labels

'''
Gets the full feature list to be used as a training dataset
    Parameters:
        feature(list): The dataset of features
        label(list): List of labels
    Returns:
        The accuarcy of the classifier
'''
def test_scholars(feature, label):
    train_features, test_features, train_labels, test_labels = train_test_split(feature, label, test_size = 0.20, random_state = 15) 
    rfc = RandomForestClassifier(n_estimators = 500, random_state = 42)   
    rfc.fit(train_features, train_labels)
    joblib.dump(rfc, "./random_forest.joblib")
    predicitons = rfc.predict(test_features)
    correct = 0
    for i in range(len(test_labels)):
        if test_labels[i] == predicitons[i]:
            correct += 1
    print(correct)
    return correct * 100.0 / len(test_labels)

'''
Store the random forrest tree classifier.
    Parameters:
        feature(list): The dataset of features
        label(list): List of labels
'''
def train_scholars(feature, label):
    rfc = RandomForestClassifier(n_estimators = 500, random_state = 42)   
    rfc.fit(feature, label)
    joblib.dump(rfc, "./random_forest.joblib")


'''
Fill in the missing data by dummy values that could help them get ignored
    Parameters:
        path(string): path of the file to be modified
'''
def fill_missing_data(path):
    df = pd.read_csv(path)
    citation_num = [i for i in df['citation#'] if i != -999]
    year = [i for i in df['year'] if i != -999]
    k_list = [i for i in df['position_k'] if i != -999]
    d_list = [i for i in df['position_d'] if i != -999]
    avgs = []
    avgs.append(sum(citation_num) / len(citation_num))
    avgs.append(sum(k_list) / len(k_list))
    avgs.append(sum(d_list) / len(d_list))
    avgs.append(sum(year) / len(year))
    print(avgs)
    df.loc[df['year'] == -999, 'year'] = avgs[3]
    df.loc[df['position_d'] == -999, 'position_d'] = avgs[2]
    df.loc[df['position_k'] == -999, 'position_k'] = avgs[1]
    df.loc[df['citation#'] == -999, 'citation#'] = avgs[0]

    df.to_csv(path, index = False)


'''
Convert a list of strings to a vector of strings that could be utilized as features
    Parameters:
        vec(list): Vector that would be converted 
'''
def convert_to_counter(vec):
    strs_list = []
    i = 0
    for v in vec:
        v = v.replace('[', '')
        v= v.replace(']', '')
        v = v.replace("',", "")
        v = v.replace("'", "")
        strs_list.append(v) 
    vectorizer = CountVectorizer()
    return vectorizer.fit_transform(strs_list).toarray()
    

# f, l = train('scholars_updated_3 copy.csv') 
# train_scholars(f, l)
# print(test_scholars(f, l))
# fill_missing_data('scholars_updated_3 copy.csv')
    