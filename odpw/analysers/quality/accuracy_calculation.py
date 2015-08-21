import mimetypes
import os
import pickle

from odpw.analysers import Analyser, AnalyserSet, process_all
from odpw.analysers.quality.analysers import analyze_resource_format
from odpw.db.dbm import PostgressDBM
from odpw.db.models import Resource, Dataset, Portal

import structlog
log =structlog.get_logger()



def format(resource_meta_datas, resource):
    score_extension = 0.0
    score_header = 0.0
    count_extension = 0.0
    count_header = 0.0
    for rmd in resource_meta_datas:
        if 'format' in rmd and rmd['format'] is not None:
            meta = rmd['format'].lower()

            # check file extensions
            if resource['url'] is not None:
                file_extension = analyze_resource_format.get_file_extension(resource['url'])
                # assuming no file extension is longer than 10 characters
                if 0 < len(file_extension) < 10:
                    ext = file_extension[1:].lower()
                    count_extension += 1

                    if analyze_resource_format.get_format(ext) in analyze_resource_format.get_format_list(meta):
                        score_extension += 1

            # check mime type
            if resource['mime']:
                cont_type = resource['mime']

                header_mime_type = cont_type.split(';')[0]
                guessed_extensions = mimetypes.guess_all_extensions(header_mime_type)
                count_header += 1
                for ext in guessed_extensions:
                    if analyze_resource_format.get_format(ext) in analyze_resource_format.get_format_list(meta):
                        score_header += 1
                        break

    result = {'content': {'count': count_extension},
              'header': {'count': count_header}}

    if count_extension > 0:
        result['content']['score'] = score_extension / count_extension
    else:
        result['content']['score'] = None
    if count_header > 0:
        result['header']['score'] = score_header / count_header
    else:
        result['header']['score'] = None
    return result


def mime_type(resource_meta_datas, resource):
    result = {'content': {'count': 0, 'score': None},
              'header': {'count': 0, 'score': None}}

    if resource['mime']:
        cont_type = resource['mime']
    else:
        return result

    header_mime_type = cont_type.split(';')[0]
    score = 0.0
    count = 0.0
    for rmd in resource_meta_datas:
        if 'mimetype' in rmd:
            count += 1
            if header_mime_type == rmd['mimetype']:
                score += 1

    result['header']['count'] = count
    if count == 0:
        result['header']['score'] = None
    else:
        result['header']['score'] = score / count
    return result


def size(resource_meta_datas, resource):
    result = {'content': {'count': 0, 'score': None},
              'header': {'count': 0, 'score': None}}

    if resource['size']:
        header_field = resource['size']
    else:
        return result

    try:
        cont_length = float(header_field)
        if cont_length <= 0:
            return result
    except Exception:
        log.warn("(%s) Cannot read content length in header of resource: %s",
                     resource['url'], header_field)
        return result

    score = 0.0
    count = 0.0
    for rmd in resource_meta_datas:
        if 'size' in rmd and rmd['size'] is not None:
            try:
                # maybe ist a string with unit at end
                meta_string = rmd['size'].split(' ')
                meta = float(meta_string[0])
            except Exception:
                continue

            # try some units, give range, maybe its a string with unit at end...
            count += 1
            # give some range..
            range = cont_length / 100.0
            if (cont_length - range) <= meta <= (cont_length + range):
                score += 1
            # check if it is in kb
            elif (cont_length - range) <= meta * 1000 <= (cont_length + range):
                score += 1
            # check if it is in kib
            elif (cont_length - range) <= meta * 1024 <= (cont_length + range):
                score += 1
            # check if it is in mb
            elif (cont_length - range) <= meta * 1000000 <= (cont_length + range):
                score += 1

    result['header']['count'] = count
    if count == 0:
        result['header']['score'] = None
    else:
        result['header']['score'] = score / count
    return result


def _compute_dict_accuracy(acc_dict):
    score_content = 0.0
    count_content = 0.0
    score_header = 0.0
    count_header = 0.0
    for url in acc_dict:
        content = acc_dict[url]['content']['score']
        header = acc_dict[url]['header']['score']
        if content is not None:
            score_content += content
            count_content += 1
        if header is not None:
            score_header += header
            count_header += 1

    result = {'content': {'count': count_content},
              'header': {'count': count_header}}
    # if field isn't in any resource-meta-data we don't give it a value
    if count_content == 0:
        result['content']['score'] = None
    else:
        result['content']['score'] = score_content / count_content

    if count_header == 0:
        result['header']['score'] = None
    else:
        result['header']['score'] = score_header / count_header
    return result





class AccuracyAnalyser(Analyser):
    def __init__(self, res_dict):
        self.res_dict = res_dict
        self.p_f = {}
        self.p_m = {}
        self.p_s = {}
        self.count = 0

    def analyse_Dataset(self, dataset):
        self.compute_accuracy(dataset)

    def getResult(self):
        # return portal accuracy
        return {
            'format': _compute_dict_accuracy(self.p_f),
            'mime_type': _compute_dict_accuracy(self.p_m),
            'size': _compute_dict_accuracy(self.p_s),
        }

    def compute_accuracy(self, dataset):
        f = {}
        m = {}
        s = {}
        resources = []
        if dataset.data:
            data = dataset.data
            for res in data.get('resources', []):
                url = res['url']
                resource = self.res_dict.get(url, None)
                if resource:
                    resource['url'] = url
                    resources.append(resource)

            for resource in resources:
                # calculate resource accuracy
                package_resources = data.get('resources', [])
                if len(package_resources) > 0:
                    try:
                        f[resource['url']] = format(package_resources, resource)
                    except Exception as ex:
                        log.warn("(%s) during url %s: %s", dataset.portal_id, resource['url'], ex.message)
                    try:
                        m[resource['url']] = mime_type(package_resources, resource)
                    except Exception as ex:
                        log.warn("(%s) during url %s: %s", dataset.portal_id, resource['url'], ex.message)
                    try:
                        s[resource['url']] = size(package_resources, resource)
                    except Exception as ex:
                        log.warn("(%s) during url %s: %s", dataset.portal_id, resource['url'], ex.message)

                    # store accuracy per resource
                    #resource.add_accuracy({
                    #    'format': f.get(resource.url, None),
                    #    'mime_type': m.get(resource.url, None),
                    #    'language': l.get(resource.url, None),
                    #    'encoding': e.get(resource.url, None),
                    #    'size': s.get(resource.url, None),
                    #    'time_span': (dt - resource.time).total_seconds()
                    #})
                    #dbm.storeResource(resource)


        # calcluate package accuracy
        self.p_f[dataset.id] = _compute_dict_accuracy(f)
        self.p_m[dataset.id] = _compute_dict_accuracy(m)
        self.p_s[dataset.id] = _compute_dict_accuracy(s)

        #metrics.add_accuracy({
        #    'format': p_f[package.id],
        #    'mime_type': p_m[package.id],
        #    'language': p_l[package.id],
        #    'encoding': p_e[package.id],
        #    'size': p_s[package.id],
        #    'time_span': max_time_span.total_seconds()
        #})

        self.count += 1
        if self.count % 10000 == 0:
            print 'processed:', self.count


if __name__ == '__main__':

    path = 'tmp/accuracy/all_res.pkl'
    with open(path, 'r') as f:
        res_dict = pickle.load(f)

    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)
    sn = 1533
    id = 'data_wu_ac_at'

    portals = dbm.getPortals(software='CKAN')
    path = 'tmp/accuracy/'
    with open(path + 'accr.pkl', 'r') as f:
        accr_dict = pickle.load(f)

    for i, p in enumerate(Portal.iter(portals)):
        pkl_path = path + p.id + '.pkl'
        if os.path.isfile(pkl_path):
            with open(pkl_path, 'r') as f:
                pkl_file = pickle.load(f)
                for k in pkl_file:
                    accr_dict[k] = pkl_file[k]

        if p.id not in accr_dict:
            accuracy = {}
            ds_analyser = AnalyserSet()
            a = ds_analyser.add(AccuracyAnalyser(res_dict))

            ds = dbm.getDatasetsAsStream(portalID=p.id, snapshot=sn)
            ds_iter = Dataset.iter(ds)
            process_all(ds_analyser, ds_iter)

            print i, 'accuracy calculated: ', p.id
            accuracy[p.id] = a.getResult()

            with open('tmp/accuracy/' + p.id + '.pkl', 'wb') as f:
                pickle.dump(accuracy, f)
