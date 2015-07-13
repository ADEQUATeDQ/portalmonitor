'''
Created on May 19, 2014

@author: max
'''

import re
from collections import Counter


import pickle
import unittest

SERVER_URL = 'http://spotlight.dbpedia.org/rest/annotate'
# 'http://localhost/rest/annotate'

class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

    def __str__(self, *args, **kwargs):
        return set.__str__(self, *args, **kwargs)

DataType = Enum(["UNKNOWN", "PHONE", "EMAIL", "ADDRESS", "STREET", "URL", "NAME",
                "CITY", "FLOAT", "INTEGER", "RESOURCE", "DATE", "ALPHA",
                "YEAR", "YEAR_MONTH", "EMPTY", "YES_NO"])

date_regex = '^(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]))\1|(?:(?:29|30)(\/|-|\.)(?:0?[1,3-9]|1[0-2])\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)0?2\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9])|(?:1[0-2]))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})$'
date_pattern = re.compile(date_regex)

date_regex2 = '^[1|2][0-9][0-9][0-9][\/-]?[0-3][0-9][\/-]?[0-3][0-9]$'
date_pattern2 = re.compile(date_regex2)

date_regex3 = '((0[1-9])|(1[0-2]))[\/-]((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))[\/-](\d{4})'
date_pattern3 = re.compile(date_regex3)

date_regex4 = '^([0-9]?[0-9][\.\/-])?([0-3]?[0-9][\.\/-])\s?[1-9][0-9]([0-9][0-9])?$'
date_pattern4 = re.compile(date_regex4)

email_regex = '[a-zA-Z0-9_\.\+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-\.]+'
email_pattern = re.compile(email_regex)

phone_regex = '^\(?\+?\d+\)?(\s?\d+)+$'
phone_pattern = re.compile(phone_regex)

address_regex = ''
address_pattern = re.compile(address_regex)

url_pattern = 'https?\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
url_regex = re.compile(url_pattern)

places = ['street', 'strasse', 'rue', 'str', 'str.', 'platz', 'allee', 'gasse', 'g.', 'blvd', 'ave', 'road']
special_symbols = ['+',' ',';','[','/',']','\\','-']
commas = [',', '.']
resource = ['://', 'www.','.jpg','.png','.gif','.html','.htm','.mp3','.doc','.pdf','.ps','.docx']
yes_no = ['yes', 'no', 'y', 'n', 'j', 'ja', 'nein', 'si', 'oui', 'da', 'njet']
units = ['m','mm','cm','km','in','ft','l','ml','t','g','kg','mg']
CACHE_FILE = '/home/max/cache'

lookup = []

def contains_number(inputString):
    return any(char.isdigit() for char in inputString)

def contains_alpha(inputString):
    return any(char.isalpha() for char in inputString)

def contains_special(inputString):
    return any(char in special_symbols for char in inputString)

def contains_commas(inputString):
    return any(char in commas for char in inputString)

def contains_ampersand(inputString):
    return any(char=='@' for char in inputString)

def contains_resource(inputString):
    for item in resource:
        if item in inputString:
            return True
    return False

def extract_datatype(cell):
    ''' '''
    if len(cell)==0 or cell=='null':
        return DataType.EMPTY
    if contains_ampersand(cell):
        if is_email(cell):
            return DataType.EMAIL
    if contains_resource(cell):
        return DataType.RESOURCE

    if contains_number(cell):
        if contains_alpha(cell):
            cell = humanize_text(cell)
            #check currency:
            pass
        elif contains_special(cell):
            #check date
            if is_date(cell):
                return DataType.DATE
        elif contains_commas(cell):
            #check
            try:
                float(cell)
            except Exception:
                pass
            return DataType.FLOAT
        else:
            try:
                int_val = int(cell)
                if int_val >=1400 and int_val <= 2100:
                    return DataType.YEAR
                if int_val >=197000 and int_val<210000:
                    return DataType.YEAR_MONTH
                if is_date(cell):
                    return DataType.DATE
            except Exception:
                pass
            return DataType.INTEGER
        return DataType.UNKNOWN
    else:
        cell = humanize_text(cell)
        if is_yes_no(cell):
            return DataType.YES_NO

        query_concept(cell)
#         items = []
#         for word in cell.split(' '):
#             item = query_concept(word)
#             if item:
#                 items.append(item)

        counter = None #Counter(items)
        if counter and len(counter):
            return counter.most_common(1)[0][0]
        else:
            return DataType.ALPHA

    return DataType.UNKNOWN

def query_concept(cell):
    lookup.append(cell)
#     try:
#         concept = extract_concepts(cell)
#         print cell
#     except Exception, e:
#         print e
    #print cell
    return 'ADDRESS'

def extract_common_datatype(cells):
    '''
    Extract common datatypes from a set of cells
    UNKNOWN=0, PHONE=1, EMAIL=2, ADDRESS=3, STREET=4, URL=5, NAME=6, CITY=7, FLOAT=8,
    INTEGER=9, RESOURCE=10, DATE=11, ALPHA=12, YEAR=13, YEAR_MONTH=14, EMPTY=15, YES_NO=16)
    '''
    types = Counter(extract_datatype(cell) for cell in cells)
    return types.most_common(1)[0][0]


def humanize_text(text):
    s1 = dequote(text)
#    if (contains_alpha(s1)):
#        s1 = ' '.join( re.findall('(\d+|\w+)', s1))
    s1 = ' '.join(s1.split())
    return s1

def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (
        s.startswith(("'", '"')) and s.endswith(("'", '"'))
        and (s[0] == s[-1])  # make sure the pair of quotes match
    ):
        s = s[1:-1]
    return s



# def query_dbpedia_spotlight(text):
#     ''' query dbpedia '''
#     annotations = []
#     try:
#         annotations = spotlight.annotate(SERVER_URL,
#                                   text,
#                                   confidence=0.4, support=20)
#     except Exception:
#         pass #nothing, really
#     return annotations

def is_yes_no(cell):
    return cell in yes_no

def is_alpha(cell):
    for c in cell:
        if not c.isalpha():
            return False
    return True

def is_alphanum(cell):
    is_digit = False
    is_alpha = False
    for c in cell:
        if c.isalpha():
            is_alpha = True
        elif c.isdigit():
            is_digit = True
    return is_digit and is_alpha

def is_street(cell):
    if address_pattern.match(cell):
        return True
    for place in places:
        if cell.contains(place):
            return True
    return False

def is_phone(cell):
    return phone_pattern.match(cell)

def is_email(cell):
    return email_pattern.match(cell)

def is_url(cell):
    return url_pattern.match(cell)

def is_digitsep(cell):
    is_digit = False
    is_sep = False
    separators = [':',',','-','/']
    for c in cell:
        if c in separators:
            is_sep = True
        elif c.isdigit():
            is_digit = True
    return is_digit and is_sep

def is_date(cell):
    if date_pattern.match(cell):
        return True
    if date_pattern2.match(cell):
        return True
    if date_pattern3.match(cell):
        return True
    if date_pattern4.match(cell):
        return True
    return False

def is_numeric(text):
    pattern = re.compile("/^\d*\.?\d*$/")
    return re.match(pattern, text)

def is_categorial(text):
    return text.isnumeric()

class HumanizeTest(unittest.TestCase):

    def test_humanize(self):
        cell = '105mm'
        cleaned = humanize_text(cell)

        cell = '12.25'
        cleaned = humanize_text(cell)

#         print cleaned

class DatatypeTest(unittest.TestCase):

    def test_unit(self):
#         cell = '105mm'
#         type = extract_datatype(cell)
#         assert type=='UNIT'

        cell = 'Gebiet/Distrikt'
        type = extract_datatype(cell)

        print type
#         assert type=='UNIT'

    def test_person(self):
        pass

    def test_dictionary(self):
        pass

    def test_date(self):
        cell = '20110101'
        type = extract_datatype(cell)
        assert type=='DATE'

        cell = '2011/01/01'
        type = extract_datatype(cell)
        assert type=='DATE'

        cell = '2011-01-01'
        type = extract_datatype(cell)
        assert type=='DATE'

        return True