'''
OPQuast provides a catalog of quality questions (see http://checklists.opquast.com/en/opendata)
This analyser verifies a subset of the queries

The following questions will be analysed
    Metadata
        24

'''
from odpw.analysers import Analyser

__author__ = 'jumbrich'

import string
from pprint import  pprint
import analyze_resource_format
from odpw.quality.interpret_meta_field import is_empty
from odpw.quality import interpret_meta_field
#no sure
def m1_22_personrole(data):
    ''' The identity and role of the person responsible for each dataset is specified
    '''



def m1_24_titledescription(data):
    '''
    Each dataset includes at least a title and a description
    '''
    if 'title' not in data or data['title'] is None:
        return 0
    # we have a non empty title


    if 'notes' in data and data['notes'] is not None:
        return 1
    elif 'description' in data and data['description'] is not None:
        return 1
    else:
        return 0


def m1_25_creationDate(data):
    '''
    A creation date is given for each dataset
    '''
    if 'metadata_created' in data and data['metadata_created'] is not None:
        return 1
    return 0


def m1_26_lastUpdate(data):
    '''
    A last-updated date is given for each dataset
    '''
    if 'metadata_modified' in data and data['metadata_modified'] is not None:
        return 1
    return 0


def m2_27_category(data):
    '''
    The datasets are categorised

    tricky, we can go for tags, category information might be in the extra meta data hidden

    maybe even tacking groups into account
    or 'type' keys
    '''

    if 'tags' in data and len(data['tags']) != 0:
        return 1

    return 0



def m2_29_language(data):
    '''
    Each dataset is accompanied by a reference to the language used

    the problem here is that language keys are in the resource fields or maybe extra
    if there are no resources, we decide to return 0
    '''


    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('language') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0


def f1_33_charsetref(data):
    '''
    Each dataset includes a reference to the charset used
    '''
    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('characterset') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0


def f1_34_format(data):
    '''
    The format of downloadable files is indicated
    '''
    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('format') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0

def f1_35_dates(data):
    print ''
    '''
    Dates are given in documented formats
    '''



def f2_36_standardformat(data):
    '''
    Dates are available in a standard format
    '''
    print ''

OPEN_FORMATS = ['csv', 'html', 'latex', 'dvi',
                'postscript', 'json', 'rdf',
                'xml', 'txt', 'ical','rss',
                'geojson',
                'ods','ttf','otf'
                'svg', 'gif', 'png']
def f2_37_atleastoneopenformat(data):
    '''
    Data is provided in at least one open format
    '''
    if 'resources' in data:
        for res in data['resources']:
            fv = res.get("format", str(None))
            format = analyze_resource_format.get_format(fv).lower()
            if format in OPEN_FORMATS:
                return 1
    return 0


def f2_38_encoding(data):
    '''
    The character encoding used in each dataset is declared

    !!! charset vs encoding issue, currently we ignore this part
    '''
    print ''

def f3_42_diffdataformats(data):
    '''
    One can access different versions of datasets
    '''
    formats = []
    if 'resources' in data:
        for res in data['resources']:
            fv = res.get("format", str(None))
            format = analyze_resource_format.get_format(fv).lower()
            formats.append(format)
    if len(formats) >1:
        return 1

    return 0

def i1_43_dataurl(data):
    '''
    The descriptive record contains a direct link to the URL of the data

    !!!
    '''
    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('url') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0

def i2_45_checksum(data):
    '''
    A checksum and/or signature is available to verify the validity of each file
    '''
    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('hash') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0

def i2_46_contact(data):
    '''
    Datasets are accompanied by at least one means of contact for the producer (or maintainer)
    '''
    author = ['author', 'author_email']
    main = ['maintainer', 'maintainer_email']

    #count emails and http urls in author fields
    for k in author:
        if k in data and not is_empty(data[k]):
            type = str(interpret_meta_field.get_type(data[k]))
            if type == 'email' or ('url' in type and 'http' in type):
                return 1
    #check emails and http urls in maintainer fields
    for k in main:
        if k in data and not is_empty(data[k]):
            type = str(interpret_meta_field.get_type(data[k]))
            if (type == 'email') or  ('url' in type and 'http' in type):
                return 1
    return 0

def l1_47_license(data):
    '''
    The datasets are accompanied by a licence

    we check for licensid and license url

    '''
    keys =['license_id', 'license_url']

    for k in keys:
        if k in data and not is_empty(data[k]):
            return 1
    return 0

def n2_56_safecharacters(data):
    '''
    The names of data files contain only alphanumeric characters or characters considered safe

    !!! strange thing
    '''
    safechars = string.letters + string.digits + " -_."
    if 'resources' in data:
        c = 0
        for res in data['resources']:
            fv = res.get('url')
            if fv:
                fv_safe = filter(lambda c: c in safechars, fv)
                if len(fv) == len(fv_safe):
                    c+=1

        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0



def t2_62_updatefrequency(data):
    '''
    An update frequency is given for each dataset
    '''
    if 'resources' in data:
        c = 0
        for res in data['resources']:

            if res.get('update_frequency') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0

def u1_65_downloadsize(data):
    '''
    The size of downloadable files is indicated
    '''

    #pprint(data)
    if 'resources' in data:
        c = 0
        for res in data['resources']:
            if res.get('size') is not None:
                c+=1
        if len(data['resources']) == 0:
            return 0
        return c/len(data['resources'])
    return 0


opquast = {
    'm1_24': m1_24_titledescription,
    'm1_25': m1_25_creationDate,
    'm1_26': m1_26_lastUpdate,
    'm2_29': m2_29_language,
    'f1_33': f1_33_charsetref,
    'f2_37': f2_37_atleastoneopenformat,
    'f3_42': f3_42_diffdataformats,
    'i1_43': i1_43_dataurl,
    'i2_45': i2_45_checksum,
    'i2_46': i2_46_contact,
    'l1_47': l1_47_license,
    'u1_65': u1_65_downloadsize
}

DS = 'ds'


class OPQuastAnalyser(Analyser):
    
    id='opquast'
    def __init__(self):
    #no of datasets as reported by the API
        self.package_count = 0

        #actual number of datasets and resource analysed
        self.size = {
            DS:0,

        }

        # the computed quality measures
        self.quality = {}

        self.stats= {}
        for q in opquast:
            self.stats[q] = []



    def getKeys(self):

        return self.freq


    def getResult(self):
        return {OPQuastAnalyser.id: self.quality}
        

    def analyse(self, dataset):
        #update package count
        self.package_count += 1

        data = dataset.data
        # if no dict, return (e.g. access denied)
        if not isinstance(data, dict):
            return

        # count only opened packages
        self.size[DS] += 1
        quality={}
        for q in opquast:
            quality[q]=opquast[q](data)
            self.stats[q].append(quality[q])
            
        
        dataset.updateQA({'qa':{OPQuastAnalyser.id:quality}})

    def computeSummary(self):
        for q in opquast:
            #print q, opquast[q]
            if len(self.stats[q]) > 0:
                self.quality[q] = sum(self.stats[q]) / float(len(self.stats[q]))
            else:
                self.quality[q] = 0

        c =0
        for q in self.quality:
            c += self.quality[q]
        self.quality['total'] = c / float(len(self.quality))
