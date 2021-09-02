from numpy.core.numeric import _outer_dispatcher
from sklearn import datasets
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from bs4 import BeautifulSoup
import requests
import joblib
import re
import csv
from train_data import train
from train_data import train_scholars

'''
Writes and saves the results of the features
    Parameters:
            keyword (string): a given keyword that would be featured
    Returns:
        the combination of the regular features and the ones that broke down the string values
'''
def get_properties(keyword):
    tutorials, other_features_1 = get_features(keyword, False)
    surveys, other_features_2 = get_features(keyword, True)

    # delete the keyword and label column
    for i in tutorials:
        del i[0]
        del i[-1]
    for i in surveys:
        del i[0]
        del i[-1]
    return tutorials + surveys, other_features_1 + other_features_2


'''
Returns the indexs of the url list to be used
'''
def get_urls(test_data, url_list):
    loaded_rf = joblib.load("./random_forest.joblib")
    res_idx = []
    res_urls = []
    # try:
    #     prediction = loaded_rf.predict(test_data)
    #     print(prediction)
    # except:
    #     print("Page not loaded")
    
    print(test_data[0])
    prediction = loaded_rf.predict(test_data)

    for i, v in enumerate(prediction):
        if v == 1 and url_list[i] not in res_urls:
            res_idx.append(i)
            res_urls.append(url_list[i])
    return res_idx

'''
Converts csv file to array
    Parameters:
        The path of the file to be converted
    Returns:
        List of the keywords
'''
def csv_to_array(path):
    keyword_data_frame = pd.read_csv(path)
    keywords = np.array(keyword_data_frame['keyword'])
    return keywords

'''
Writes and saves the results of prediction that is classified as a 'good' paper
    Parameters:
            the list of keywords to be classified from google scholars
'''
def save_tutorials(keyword_list):
    for k in keyword_list:
        data_list, other_data = get_properties(k)
        other_data_np = np.array(other_data)
        url_res_idx = get_urls(data_list, other_data_np[:, 0])
        with open('expanded_tutorials_test.csv', 'a', encoding="utf-8") as file:
            writer = csv.writer(file)
            for idx in url_res_idx:
                row = [k] 
                row = row + other_data[idx]
                writer.writerow(row)

'''
Returns the sum of two decimal numbers in binary digits.

    Parameters:
            keyword (string): a given keyword that would be featured
            is_survey (bool): flag that determines if the given method is looking for a survey or a tutorial
    Returns:
            all_features (list): A 2d array of all the features that are trained.
            other_features (list): Literal string values that could be used as a feature themselves
'''
def get_features(keyword, is_survey):
    keyword = keyword.lower()
    url = 'https://urldefense.com/v3/__https://scholar.google.com/scholar?hl=en&as_sdt=0*2C14&q=__;JQ!!DZ3fjg!rrDiili1TTe4o1ecLFFzvV0EFmORL4SEZJqeXN7gJ5yJW90yZDa52muaKp9hJA7wn8qA$ '
    url += keyword.replace(' ', '+')
    if is_survey:
        url += '+survey'
    else:
        url += '+tutorial'

    words = keyword.split()

    headers = {'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36"}
    result = requests.get(url, headers = headers)

    soup = BeautifulSoup(result.content, 'html.parser')

    all_features = []
    other_features = []

    idx = 1 
    for item in soup.select('[data-lid]'):
        feature = []
        
        feature.append(keyword)
        feature.append(idx)

        idx += 1
        title_raw = item.select('h3')[0].get_text()
        title = title_raw.lower()
        if len(item.select('.gs_rs')) == 0:
            summary_ = ""
        else:
            summary_ = item.select('.gs_rs')[0].get_text()


        file_format = item.select('.gs_fl')[0].get_text()
        citation = item.select('.gs_fl')[-1].get_text().split()
        if len(citation) > 2 and citation[2].isdigit():
            citation_num = int(citation[2])
        else:
            citation_num = -1

        expl = item.select('.gs_a')[0].get_text(strip=True)
        paper = expl.split('-')

        info = paper[-2]
        author = paper[0]
        author = author.replace(u'\xa0', u' ')
        if info[-5:-1].isdigit():
            year = int(info[-5:-1])
        else:
            year = -1
        
        # website's published year
        feature.append(year)
        # website contains pdf file
        if file_format.startswith('[PDF]') or title.startswith('[PDF]'):
            feature.append(True)
        else:
            feature.append(False)

        # website contains html format
        if file_format.startswith('[HTML]') or title.startswith('[HTML]'):
            feature.append(True)
        else:
            feature.append(False)

        is_book = item.select('span.gs_ct1')
        if len(is_book) > 0 and is_book[0].get_text() == '[BOOK]':
           feature.append(True)
        else:
            feature.append(False)

        # website is by education institute.
        flag = True
        if file_format.endswith('.edu'):
            flag = False
            feature.append(True)
        else: 
            feature.append(False)

        if file_format.endswith('.org'):
            flag = False
            feature.append(True)
        else: 
            feature.append(False)

        if file_format.endswith('.com'):
            flag = False
            feature.append(True)
        else: 
            feature.append(False)
        
        # the website ends with a different domain
        if flag and '.' in file_format:
            feature.append(True)
        else:
            feature.append(False)

        # the keyword as a whole is in the title
        if keyword in title:
            feature.append(True)
        else:
            feature.append(False)

        # all keywords in the title
        title_list = title.split()
        if all(w in title_list for w in words):
            feature.append(True)
        else:
            feature.append(False)

        # number of citations
        feature.append(citation_num)

        # length of the title
        title_length = len(title_list)

        # occurences of the words from the keyword in the title
        cnt = 0
        for w in words:
            temp = title.count(w)
            cnt += temp
            title_length -= temp 

        # number of words in the title
        feature.append(title_length)
        feature.append(cnt)
        #occureneces of the keyword in the summary
        feature.append(summary_.count(keyword))
        # check if ':' is in the title 
        feature.append(':' in title) 
        
        type_of_doc = ['survey', 'tutorial', 'review']

        # if the title contains any of the type of documents
        if any(t in title for t in type_of_doc):
            feature.append(True)
        else:
            feature.append(False)
        
        # title has 'tutorial' in the title
        if type_of_doc[0] in title:
            feature.append(True)
        else:
            feature.append(False)

        # title has 'survey' in the title
        if type_of_doc[1] in title:
            feature.append(True)
        else:
            feature.append(False)

        # title has 'review' in the title
        if type_of_doc[2] in title:
            feature.append(True)
        else:
            feature.append(False)
        # default label
        feature.append(0.00)
        other_features.append([item.select('a')[0]['href'], author.split(','), re.sub("[\(\[].*?[\)\]]", "", title_raw), year, citation_num])
        all_features.append(feature)
    return all_features, other_features


f, l = train('google_scholars\scholars_updated_3 copy.csv') 
print(f[0])
train_scholars(f, l)
keyword_list = csv_to_array('stored_keyword_modified.csv')
save_tutorials(keyword_list)