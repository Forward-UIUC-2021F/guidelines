# This code is created based on Jiaying Li's code for achieving researcher's paper counts
# which is shared on Slack on Feb 9th, 2021.
from fuzzywuzzy import fuzz
import requests
import json
import glob
import csv
import re


# API Request variables
HEADERS = {"Ocp-Apim-Subscription-Key": "b0dbb13065164b6f8bcec38e89df091e"} # Specific to Liam Xu
QUERYSTRING = {"mode":"json%0A"}
PAYLOAD = "{}"

def _normalize_author_name(name):
    name = name.strip().replace(', Ph.D.','').replace('Dr. ','')
    name = name.lower().replace('.', '').replace('-', ' ').replace('(', '').replace(')', '').replace(', ', ' ').replace(',',' ')

    return name

def _normalize_institution_name(affi):
    affi = affi.lower().replace('--',' ').replace('-', ' ')

    return affi


def _get_abstract_from_IA(index_length, inverted_index):
    token_list = [None] * index_length
    for token in inverted_index:
        for ind in inverted_index[token]:
            token_list[ind] = token
    token_list = list(filter(None, token_list))
    abstract = u' '.join(token_list)
    return abstract


def _get_author_by_id(author_id):

    # Search for author using name and institution id
    author_req_attrs = 'Id,AuN,LKA.AfN,CC'
    author_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?&count=1&expr=Id={}&attributes={}".format(author_id, author_req_attrs)
    response = requests.request("GET", author_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)
    response = response.json()

    ret_auth = None
    for author in response["entities"]:
        if "LKA" in author.keys() and author["Id"] == author_id:

            author_renamed_fields = {}
            author_renamed_fields['name'] = author['AuN']
            author_renamed_fields['id'] = author['Id']
            author_renamed_fields['num_citations'] = author['CC']

            if author["LKA"]["AfN"]:
                author_renamed_fields['inst'] = author["LKA"]["AfN"]

            print("Found author: ")
            print(author_renamed_fields)
            return author_renamed_fields




# Id in MAG database
def mag_get_author(author_name, author_inst):
    print("Looking for author...")
    author_name = _normalize_author_name(author_name)
    author_inst = _normalize_institution_name(author_inst)

    # Search author papers
    paper_req_attrs = 'DN,Y,CC,AA.AuId,AA.AuN,AA.AfN'
    papers_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?&count=1&expr=Composite(AND(AA.AuN='{}',AA.AfN='{}'))&attributes={}".format(author_name, author_inst, paper_req_attrs)

    response = requests.request("GET", papers_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)

    entity_list = json.loads(response.text)['entities']
    print("Extracting author id from paper")
    print(entity_list[0])

    # Match author name with returned AA.AuN to determine author ID
    author_mag_id = None
    max_ratio = 0
    for author in entity_list[0]['AA']:
        curr_ratio = fuzz.token_sort_ratio(author_name, author['AuN'])
        #  + fuzz.token_sort_ratio(author_inst, author['AfN'])

        if(curr_ratio > max_ratio):
            author_mag_id = author['AuId']
            max_ratio = curr_ratio


    if author_mag_id is None:
        raise Exception("Author not found")
    else:
        return _get_author_by_id(author_mag_id)



def _create_paper_json(entity, author_id=None):
    paper = {}

    if 'IA' in entity:
        index_length = entity['IA']['IndexLength']
        inverted_index = entity['IA']['InvertedIndex']
        paper['abstract'] = _get_abstract_from_IA(index_length, inverted_index)


    paper['mag_id'] = entity['Id']

    if 'DN' in entity:
        paper['title'] = entity['DN']

    if 'Y' in entity:
        paper['year'] = entity['Y']

    if 'CC' in entity:
        paper['num_citations'] = entity['CC']

    if 'ECC' in entity:
        paper['est_citations'] = entity['ECC']

    if author_id is not None:
        temp_author = list(filter(lambda a: a['AuId'] == author_id, entity['AA']))[0]
        paper['author'] = {}
        paper['author']['order'] = temp_author['S']

        if 'AfN' in temp_author:
            paper['author']['inst'] = temp_author['AfN']

    return paper


def mag_get_author_papers(author_id, num_papers):

    print("Searching papers including author id " + str(author_id) + "... ")

    # Query author's papers
    paper_req_attrs = 'Id,DN,IA,CC,Y,AA.AuId,AA.S,AA.AfN'
    papers_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?&count={}&expr=Composite(AA.AuId={})&attributes={}".format(str(num_papers), str(author_id), paper_req_attrs)

    response = requests.request("GET", papers_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)

    entities = json.loads(response.text)['entities']

    # Convert and store paper information
    papers = []
    for entity in entities:
        paper = _create_paper_json(entity, author_id)

        if paper is not None:
            papers.append(paper)

    return papers



def mag_get_paper(id):

    # Query author's papers
    num_res = 1
    paper_req_attrs = 'Id,DN,IA,CC,Y,AA.AuId,AA.S,AA.AfN'

    papers_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?&count={}&expr=Id={}&attributes={}".format(str(num_res), str(id), paper_req_attrs)

    response = requests.request("GET", papers_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)

    try:
        entity = json.loads(response.text)['entities'][0]
        return _create_paper_json(entity)

    except:
        print("API error: ")
        print(response.text)


def mag_get_papers(ids):
    paper_req_attrs = 'Id,DN,IA,CC,Y,AA.AuId,AA.S,AA.AfN'
    filter_func = lambda t: 'IA' in t
    return _mag_get_papers_helper(ids, paper_req_attrs, filter_func)


def mag_get_paper_citations(ids):
    paper_req_attrs = 'Id,ECC'
    filter_func = lambda t: 'ECC' in t and t['ECC'] > 0
    return _mag_get_papers_helper(ids, paper_req_attrs, filter_func)


def _mag_get_papers_helper(ids, paper_req_attrs, filter_func):

    # Query author's papers
    num_res = len(ids)
    match_cond = "Or(" + ",".join(["Id=" + str(id) for id in ids]) + ")"
    papers_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/evaluate?&count={}&expr={}&attributes={}".format(str(num_res), match_cond, paper_req_attrs)

    response = requests.request("GET", papers_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)

    try:
        entities = json.loads(response.text)['entities']
        entities = [t for t in entities if filter_func(t)]
    except:
        print("API error")
        print(response.text)
        return

    # Convert and store paper information
    papers = []
    for entity in entities:
        paper = _create_paper_json(entity)

        if paper is not None:
            papers.append(paper)

    return papers


def mag_get_institution(inst_name):

    num_res = 1
    query = inst_name
    inst_req_attrs = 'Id,AfN'
    inst_name = _normalize_institution_name(inst_name)

    match_cond = "AfN='{}'".format(inst_name)
    inst_request = "https://api.labs.cognitive.microsoft.com/academic/v1.0/interpret?query={}&complete=0&normalize=1&attributes={}&offset=0&timeout=2000&count=1&entityCount=10".format(query, inst_req_attrs)

    response = requests.request("GET", inst_request, headers=HEADERS, data=PAYLOAD, params=QUERYSTRING)

    # print(type(response.text), response.text)

    univ_name_delim = "canonical="
    delim_idx = response.text.find(univ_name_delim)

    if delim_idx == -1:
        univ_name_delim = "query"
        delim_idx = response.text.find(univ_name_delim)
        univ_start_idx = delim_idx + len(univ_name_delim) + 3
    else:
        univ_start_idx = delim_idx + len(univ_name_delim) + 2

    univ_end_idx = univ_start_idx

    while response.text[univ_end_idx] not in ["\\", "\""]:
        univ_end_idx += 1

    # print("Start and end: ", univ_start_idx, univ_end_idx)
    return response.text[univ_start_idx:univ_end_idx]


def filter_inst_name(query_inst_name):
    res = ""
    for c in query_inst_name:
        if c.isalpha() or c in [" ", "-", "/", "–"]:
            res += c

    return res


if __name__ == '__main__':

    # query_author_inst = 'University of Illinois at Urbana Champaign'
    query_author_inst = 'Caltech'
    # mag_inst_name = mag_get_institution(query_author_inst)
    # print(mag_inst_name)

    query_author_name = 'Aaron ames'
    # query_author_name = "Kevin Chenchuan Chang"

    # author = _get_author_by_id(2121939561)
    # author = mag_get_author(query_author_name, mag_inst_name)
    author = mag_get_author(query_author_name, query_author_inst)
    # exit()

    papers = []
    # papers = mag_get_author_papers(author['id'], 5)

    for p_inf in papers:
        print(json.dumps(p_inf, indent=4))
        print('\n\n\n')

    print("Obtained " + str(len(papers)) + " papers.")