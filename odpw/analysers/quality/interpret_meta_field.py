
from odpw.utils import data_utils
import dateutil
import re
#from enum import Enum

http_url_pattern = 'http\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
http_url_regex = re.compile(http_url_pattern)

https_url_pattern = 'https\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
https_url_regex = re.compile(https_url_pattern)

ftp_url_pattern = 'ftp\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
ftp_url_regex = re.compile(ftp_url_pattern)

relative_url_pattern = '\/?[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}'
relative_url_regex = re.compile(relative_url_pattern)

#Type = Enum('empty', 'phone', 'email', 'http_url', 'https_url', 'ftp_url', 'relative_url', 'float', 'integer', 'date', 'yes_no', 'word', 'text', 'string', 'bool', 'unknown', 'list', 'dict')



def is_string(cell):
    if isinstance(cell, basestring):
        return True
    else:
        return False


def is_mail_to(cell):
    if data_utils.contains_ampersand(cell):
        if data_utils.is_email(cell):
            return True
        elif cell.startswith("mailto:"):
            return data_utils.is_email(cell[len("mailto:"):])
    return False


def is_empty(cell):
    if None == cell:
        return True
    if isinstance(cell, unicode) or isinstance(cell, str):
        return len(cell) == 0 or cell == 'null' or cell == 'NA'
    if isinstance(cell, list) or isinstance(cell, dict):
        return len(cell) == 0
    else:
        return False


def is_float(cell):
    try:
        float(cell)
        return True
    except Exception:
        pass
    return False


def is_integer(cell):
    try:
        int(cell)
        return True
    except Exception:
        pass
    return False


def is_date(cell):
    if data_utils.is_date(cell):
        return True
    try:
        dateutil.parse(cell)
        return True
    except Exception:
        pass
    return False


def is_http_url(cell):
    return http_url_regex.match(cell)


def is_https_url(cell):
    return https_url_regex.match(cell)


def is_ftp_url(cell):
    return ftp_url_regex.match(cell)


def is_relative_url(cell):
    if relative_url_regex.match(cell) and len(cell.split(' ')) == 1 and len(cell.split(',')) == 1:
        return True
    return False


def is_word(cell):
    if len(cell.split(' ')) == 1 and len(cell.split(',')) == 1 and len(cell.split('.')) == 1 and len(
            cell.split(';')) == 1:
        return True
    return False


def is_text(cell):
    if len(cell.split(' ')) > 1 or len(cell.split(',')) > 1 or len(cell.split('.')) > 1 or len(
            cell.split(';')) > 1:
        return True
    return False


def is_bool(cell):
    if cell in [True, False]:
        return True
    if cell in ['true', 'True', 't', 'y', 'yes', 'false', 'False', 'f', 'n', 'no']:
        return True
    return False


def get_type(cell):
    if is_string(cell):
        if is_empty(cell): return 'empty'
        if cell == 'dict': return 'dict'
        if cell == 'list': return 'list'
        if is_mail_to(cell): return 'email'
        if is_http_url(cell): return 'http_url'
        if is_https_url(cell): return 'https_url'
        if is_ftp_url(cell): return 'ftp_url'
        if is_relative_url(cell): return 'relative_url'
        if is_integer(cell): return 'integer'
        if is_float(cell): return 'float'
        if data_utils.is_yes_no(cell): return 'yes_no'
        if is_bool(cell): return 'bool'
        if is_date(cell): return 'date'
        if data_utils.is_phone(cell): return 'phone'
        if is_word(cell): return 'word'
        if is_text(cell): return 'text'
        return 'string'
    if is_integer(cell): return 'integer'
    if is_float(cell): return 'float'
    if is_bool(cell): return 'bool'
    return 'unknown'