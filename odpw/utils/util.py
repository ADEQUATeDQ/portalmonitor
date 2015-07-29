import time
import json
from ast import literal_eval


__author__ = 'jumbrich'

import ckanapi
import urlparse
import urlnorm
from datetime import datetime
import sys
import requests.exceptions
import exceptions
import math
from collections import defaultdict


import logging
log = logging.getLogger(__name__)
from structlog import get_logger, configure
from structlog.stdlib import LoggerFactory
configure(logger_factory=LoggerFactory())
log = get_logger()

class ErrorHandler():

    exceptions=defaultdict(long)
    
    @classmethod
    def handleError(cls, log, msg=None, exception=None, **kwargs):
        name=type(exception).__name__
        cls.exceptions[name] +=1
        log.error(msg,exctype=type(exception), excmsg=exception.message,**kwargs)
    
    @classmethod
    def printStats(cls):
        print "\n -------------------------"
        print "  Numbers of Exceptions:"
        for exc, count in cls.exceptions.iteritems():
            print " ",exc, count
        print "\n -------------------------"


def getPackage(api, apiurl, id):
    ex =None
    package = None
    try:
        package = api.action.package_show(id=id)
        return package
    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListRemoteCKAN", exception=e, exc_info=True)
        ex = e

    ex1=None
    try:
        url = urlparse.urljoin(apiurl, "/api/2/rest/dataset/" + id)
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            package = resp.json()
            return package

    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListHTTPGet", exception=e, exc_info=True)
        ex1=e

    if ex and ex1:
        raise ex1
    else:
        return package


def getPackageList(apiurl):
    """ Try api 3 and api 2 to get the full package list"""
    ex =None
    
    status=200
    package_list=set([])
    try:
        api = ckanapi.RemoteCKAN(apiurl, get_only=True)
        
        start=0
        steps=100000
        while True:
            p_l = api.action.package_list(limit=steps, offset=start)
            if p_l:
                c=len(package_list)
                steps= c if start==0 else steps
                package_list.update(p_l)
                if c == len(package_list):
                    #no new packages
                    break
                start+=steps
            else:
                break
    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListRemoteCKAN", exception=e, exc_info=True)
        ex = e
    
    ex1=None
    try:
        url = urlparse.urljoin(apiurl, "/api/2/rest/dataset")
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            p_l = resp.json()
            package_list.update(p_l)
        else:
            status = resp.status_code
    except Exception as e:
        ErrorHandler.handleError(log, "getPackageListHTTPGet", exception=e, exc_info=True)
        ex1=e
    
    if len(package_list) == 0:
        if ex1:
            raise ex1
        if ex:
            raise ex
    return package_list, status


def extras_to_dicts(datasets):
    for dataset in datasets:
        extras_to_dict(dataset)
        

def extras_to_dict(dataset):
    extras_dict = {}
    extras = dataset.get("extras", [])
    if isinstance(extras, list):
        for extra in extras:
            key = extra["key"]
            value = extra["value"]
            assert key not in extras_dict
            extras_dict[key] = value
        dataset["extras"] = extras_dict

def computeID(url):
    try:
        up = urlparse.urlparse(urlnorm.norm(url))
        return up.hostname
    except Exception as e:
        return None

tld2iso3={}
iso3list=[]
import os
import os, odpw
template = os.path.join(odpw.__path__[0], 'resources', 'initial_data.json')
with open(template) as f:
    countries = json.load(f)
    for c in countries:
        data = c['fields']
        tld = data['tld'].replace(".","") if data['tld'] else data['tld'] 
        iso3list.append(data['iso3'])
        tld2iso3[tld] = data['iso3']
        
    print tld2iso3

def getISO3(tld):
    iso3= tld2iso3[tld]
    if not iso3:
        return ""
    
# The mappings
nameorgs = {
    # New top level domains as described by ICANN
    # http://www.icann.org/tlds/
    "aero": "air-transport industry",
    "arpa": "Arpanet",
    "biz": "business",
    "com": "commercial",
    "coop": "cooperatives",
    "edu": "educational",
    "gov": "government",
    "info": "unrestricted `info'",
    "int": "international",
    "mil": "military",
    "museum": "museums",
    "name": "`name' (for registration by individuals)",
    "net": "networking",
    "org": "non-commercial",
    "pro": "professionals",
    # These additional ccTLDs are included here even though they are not part
    # of ISO 3166.  IANA has 5 reserved ccTLDs as described here:
    #
    # http://www.iso.org/iso/en/prods-services/iso3166ma/04background-on-iso-3166/iso3166-1-and-ccTLDs.html
    #
    # but I can't find an official list anywhere.
    #
    # Note that `uk' is the common practice country code for the United
    # Kingdom.  AFAICT, the official `gb' code is routinely ignored!
    #
    # <D.M.Pick@qmw.ac.uk> tells me that `uk' was long in use before ISO3166
    # was adopted for top-level DNS zone names (although in the reverse order
    # like uk.ac.qmw) and was carried forward (with the reversal) to avoid a
    # large-scale renaming process as the UK switched from their old `Coloured
    # Book' protocols over X.25 to Internet protocols over IP.
    #
    # See <url:ftp://ftp.ripe.net/ripe/docs/ripe-159.txt>
    #
    # Also, `su', while obsolete is still in limited use.
    "ac": "Ascension Island",
    "gg": "Guernsey",
    "im": "Isle of Man",
    "je": "Jersey",
    "uk": "United Kingdom (common practice)",
    "su": "Soviet Union (still in limited use)",
}

countries = {
    "af": "Afghanistan",
    "al": "Albania",
    "dz": "Algeria",
    "as": "American Samoa",
    "ad": "Andorra",
    "ao": "Angola",
    "ai": "Anguilla",
    "aq": "Antarctica",
    "ag": "Antigua and Barbuda",
    "ar": "Argentina",
    "am": "Armenia",
    "aw": "Aruba",
    "au": "Australia",
    "at": "Austria",
    "az": "Azerbaijan",
    "bs": "Bahamas",
    "bh": "Bahrain",
    "bd": "Bangladesh",
    "bb": "Barbados",
    "by": "Belarus",
    "be": "Belgium",
    "bz": "Belize",
    "bj": "Benin",
    "bm": "Bermuda",
    "bt": "Bhutan",
    "bo": "Bolivia",
    "ba": "Bosnia and Herzegowina",
    "bw": "Botswana",
    "bv": "Bouvet Island",
    "br": "Brazil",
    "io": "British Indian Ocean Territory",
    "bn": "Brunei Darussalam",
    "bg": "Bulgaria",
    "bf": "Burkina Faso",
    "bi": "Burundi",
    "kh": "Cambodia",
    "cm": "Cameroon",
    "ca": "Canada",
    "cv": "Cape Verde",
    "ky": "Cayman Islands",
    "cf": "Central African Republic",
    "td": "Chad",
    "cl": "Chile",
    "cn": "China",
    "cx": "Christmas Island",
    "cc": "Cocos (Keeling) Islands",
    "co": "Colombia",
    "km": "Comoros",
    "cg": "Congo",
    "cd": "Congo, The Democratic Republic of the",
    "ck": "Cook Islands",
    "cr": "Costa Rica",
    "ci": "Cote D'Ivoire",
    "hr": "Croatia",
    "cu": "Cuba",
    "cy": "Cyprus",
    "cz": "Czech Republic",
    "dk": "Denmark",
    "dj": "Djibouti",
    "dm": "Dominica",
    "do": "Dominican Republic",
    "tp": "East Timor",
    "ec": "Ecuador",
    "eg": "Egypt",
    "sv": "El Salvador",
    "gq": "Equatorial Guinea",
    "er": "Eritrea",
    "ee": "Estonia",
    "et": "Ethiopia",
    "fk": "Falkland Islands (Malvinas)",
    "fo": "Faroe Islands",
    "fj": "Fiji",
    "fi": "Finland",
    "fr": "France",
    "gf": "French Guiana",
    "pf": "French Polynesia",
    "tf": "French Southern Territories",
    "ga": "Gabon",
    "gm": "Gambia",
    "ge": "Georgia",
    "de": "Germany",
    "gh": "Ghana",
    "gi": "Gibraltar",
    "gr": "Greece",
    "gl": "Greenland",
    "gd": "Grenada",
    "gp": "Guadeloupe",
    "gu": "Guam",
    "gt": "Guatemala",
    "gn": "Guinea",
    "gw": "Guinea-Bissau",
    "gy": "Guyana",
    "ht": "Haiti",
    "hm": "Heard Island and Mcdonald Islands",
    "va": "Holy See (Vatican City State)",
    "hn": "Honduras",
    "hk": "Hong Kong",
    "hu": "Hungary",
    "is": "Iceland",
    "in": "India",
    "id": "Indonesia",
    "ir": "Iran, Islamic Republic of",
    "iq": "Iraq",
    "ie": "Ireland",
    "il": "Israel",
    "it": "Italy",
    "jm": "Jamaica",
    "jp": "Japan",
    "jo": "Jordan",
    "kz": "Kazakstan",
    "ke": "Kenya",
    "ki": "Kiribati",
    "kp": "Korea, Democratic People's Republic of",
    "kr": "Korea, Republic of",
    "kw": "Kuwait",
    "kg": "Kyrgyzstan",
    "la": "Lao People's Democratic Republic",
    "lv": "Latvia",
    "lb": "Lebanon",
    "ls": "Lesotho",
    "lr": "Liberia",
    "ly": "Libyan Arab Jamahiriya",
    "li": "Liechtenstein",
    "lt": "Lithuania",
    "lu": "Luxembourg",
    "mo": "Macau",
    "mk": "Macedonia, The Former Yugoslav Republic of",
    "mg": "Madagascar",
    "mw": "Malawi",
    "my": "Malaysia",
    "mv": "Maldives",
    "ml": "Mali",
    "mt": "Malta",
    "mh": "Marshall Islands",
    "mq": "Martinique",
    "mr": "Mauritania",
    "mu": "Mauritius",
    "yt": "Mayotte",
    "mx": "Mexico",
    "fm": "Micronesia, Federated States of",
    "md": "Moldova, Republic of",
    "mc": "Monaco",
    "mn": "Mongolia",
    "ms": "Montserrat",
    "ma": "Morocco",
    "mz": "Mozambique",
    "mm": "Myanmar",
    "na": "Namibia",
    "nr": "Nauru",
    "np": "Nepal",
    "nl": "Netherlands",
    "an": "Netherlands Antilles",
    "nc": "New Caledonia",
    "nz": "New Zealand",
    "ni": "Nicaragua",
    "ne": "Niger",
    "ng": "Nigeria",
    "nu": "Niue",
    "nf": "Norfolk Island",
    "mp": "Northern Mariana Islands",
    "no": "Norway",
    "om": "Oman",
    "pk": "Pakistan",
    "pw": "Palau",
    "ps": "Palestinian Territory, Occupied",
    "pa": "Panama",
    "pg": "Papua New Guinea",
    "py": "Paraguay",
    "pe": "Peru",
    "ph": "Philippines",
    "pn": "Pitcairn",
    "pl": "Poland",
    "pt": "Portugal",
    "pr": "Puerto Rico",
    "qa": "Qatar",
    "re": "Reunion",
    "ro": "Romania",
    "ru": "Russian Federation",
    "rw": "Rwanda",
    "sh": "Saint Helena",
    "kn": "Saint Kitts and Nevis",
    "lc": "Saint Lucia",
    "pm": "Saint Pierre and Miquelon",
    "vc": "Saint Vincent and the Grenadines",
    "ws": "Samoa",
    "sm": "San Marino",
    "st": "Sao Tome and Principe",
    "sa": "Saudi Arabia",
    "sn": "Senegal",
    "sc": "Seychelles",
    "sl": "Sierra Leone",
    "sg": "Singapore",
    "sk": "Slovakia",
    "si": "Slovenia",
    "sb": "Solomon Islands",
    "so": "Somalia",
    "za": "South Africa",
    "gs": "South Georgia and the South Sandwich Islands",
    "es": "Spain",
    "lk": "Sri Lanka",
    "sd": "Sudan",
    "sr": "Suriname",
    "sj": "Svalbard and Jan Mayen",
    "sz": "Swaziland",
    "se": "Sweden",
    "ch": "Switzerland",
    "sy": "Syrian Arab Republic",
    "tw": "Taiwan, Province of China",
    "tj": "Tajikistan",
    "tz": "Tanzania, United Republic of",
    "th": "Thailand",
    "tg": "Togo",
    "tk": "Tokelau",
    "to": "Tonga",
    "tt": "Trinidad and Tobago",
    "tn": "Tunisia",
    "tr": "Turkey",
    "tm": "Turkmenistan",
    "tc": "Turks and Caicos Islands",
    "tv": "Tuvalu",
    "ug": "Uganda",
    "ua": "Ukraine",
    "ae": "United Arab Emirates",
    "gb": "United Kingdom",
    "us": "United States",
    "um": "United States Minor Outlying Islands",
    "uy": "Uruguay",
    "uz": "Uzbekistan",
    "vu": "Vanuatu",
    "ve": "Venezuela",
    "vn": "Viet Nam",
    "vg": "Virgin Islands, British",
    "vi": "Virgin Islands, U.S.",
    "wf": "Wallis and Futuna",
    "eh": "Western Sahara",
    "ye": "Yemen",
    "yu": "Yugoslavia",
    "zm": "Zambia",
    "zw": "Zimbabwe",
}

all = nameorgs.copy()
all.update(countries)


def getCountry(url):
    try:
        
        url_elements = urlparse.urlparse(url).netloc.split(".")

        tld = ".".join(url_elements[-2:])
        if tld in all:
            return all[tld]
        elif url_elements[-1] in all:
            return all[url_elements[-1]]
        else:
            return "unknown"
    except Exception as e:
        print e, 'for', url
        return 'error'




def getExceptionCode(e):
    #connection erorrs
    try:
        
        if isinstance(e,requests.exceptions.ConnectionError):
            return 702
        if isinstance(e,requests.exceptions.ConnectTimeout):
            return 703
        if isinstance(e,requests.exceptions.ReadTimeout):
            return 704
        if isinstance(e,requests.exceptions.HTTPError):
            return 705
        if isinstance(e,requests.exceptions.TooManyRedirects):
            return 706
        if isinstance(e,requests.exceptions.Timeout):
            return 707
        if isinstance(e,ckanapi.errors.CKANAPIError):
            try:
                err = literal_eval(e.extra_msg)
                return err[1]
            except Exception:
                return 708
        
        #if isinstance(e,requests.exceptions.RetryError):
        #    return 708

        #parser errors
        if isinstance(e, exceptions.ValueError):
            return 801

        #format errors
        if isinstance(e,urlnorm.InvalidUrl):
            return 901
        if isinstance(e,requests.exceptions.InvalidSchema):
            return 902
        if isinstance(e,requests.exceptions.MissingSchema):
            return 903
        else:
            return 600
    except Exception as e:
        log.error("Get Exception code", exctype=type(e), excmsg=e.message,exc_info=True)
        return 601

def getExceptionString(e):
    try:
        if isinstance(e,ckanapi.errors.CKANAPIError):
            try:
                err = literal_eval(e.extra_msg)
                return str(type(e))+":"+str(err[2])
            except Exception:
                return str(type(e))+":"+str(e.extra_msg)
        else:
            return str(type(e))+":"+str(e.message)
    except Exception as e:
        log.error("Get Exception string", exctype=type(e), excmsg=e.message,exc_info=True)
        return 601


def getSnapshot(args):
    if args.snapshot:
        return args.snapshot
    else:
        now = datetime.now()
        y=now.isocalendar()[0]
        w=now.isocalendar()[1]
        sn=str(y)+'-'+str(w)
        if not args.ignore:
            while True:
                choice = raw_input("WARNING: Do you really want to use the current date as snapshot "+sn+"?: (Y/N)").lower()
                if choice == 'y':
                    break
                elif choice == 'n':
                    return None
                else:
                    sys.stdout.write("Please respond with 'y' or 'n' \n")
        return sn




def convertSize(size):
    size_name = ("B","KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size,1024)))
    p = math.pow(1024,i)
    s = round(size/p,2)
    if (s > 0):
        return '%s %s' % (s,size_name[i])
    else:
        return '0B'



def extractMimeType(ct):
    if ";" in ct:
        return str(ct)[:ct.find(";")].strip()
    return ct.strip()


from datetime import timedelta

def timer(delta):
    hours, rem = divmod(delta, 3600)
    minutes, seconds = divmod(rem, 60)
    return ("{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))


def progressIterator(iterable, total, steps):
    c=0
    start= time.time()
    for element in iterable:
        c+=1
        if c%steps ==0:
            elapsed = (time.time() - start)
            progressIndicator(c, total, elapsed=elapsed)
        
        yield element

def progressIndicator(processed, total,bar_width=20,elapsed=None, interim=None, label=None):
    
    if total!=0:
        percent = float(processed) / total
    else:
        percent =1.0
    hashes = '#' * int(round(percent * bar_width))
    spaces = ' ' * (bar_width - len(hashes))
    
    el_str=""
    if elapsed:
        el_str= "runtime: "+timer(elapsed)
        #str(timedelta(seconds=elapsed))
    it_str=""
    if interim:
        it_str="interim: "+timer(interim)
        #str(timedelta(seconds=interim))

    l= label if label else 'Progress'
    sys.stdout.write("\r{6}: {1}% [{0}] ({2}/{3}) {4} {5}".format(hashes + spaces, int(round(percent * 100)), processed, total, el_str,it_str, l))
    sys.stdout.flush()

def head(url, redirects=0, props=None):
    if not props:
        props={}
        props['mime']=None
        props['size']=None
        props['redirects']=None
        props['status']=None
        props['header']=None
        props['exception']=None
    
    
    headResp = requests.head(url=url,timeout=(2, 30.0), allow_redirects=True)#con, read -timeout

    header_dict = dict((k.lower(), v) for k, v in dict(headResp.headers).iteritems())
    
    if 'content-type' in header_dict:
        props['mime']=extractMimeType(header_dict['content-type'])
    else:
        props['mime']='missing'
    
    props['status']=headResp.status_code
    props['header']=header_dict
    if headResp.status_code == requests.codes.ok:
        if 'content-length' in header_dict:
            props['size']=header_dict['content-length']
        else:
            props['size']=0

    if headResp.status_code in [ requests.codes.moved, requests.codes.see_other]:
        moved_url = header_dict['location']
        if redirects == 0:
            props['redirects']=[]
        props['redirects'].append(header_dict)
        if redirects < 3:
            redirects += 1
            if moved_url:
                return head(url=moved_url, redirects=redirects,props=props)
            else:
                props['status']=778
        else:
            props['status']=777
    
    return props

if __name__ == '__main__':
    logging.basicConfig()
    log = get_logger()
    
    try:
        raise ValueError('A very specific bad thing happened')
    except Exception as e:
        ErrorHandler.handleError(log, "test", exception=e, line="line")
        
    print ErrorHandler.exceptions
