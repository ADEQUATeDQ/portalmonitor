import mimetypes

import pycountry

from analysis.analysers import analyze_resource_format
from db.model.Package import Package
from db.model.Resource import Resource


__author__ = 'neumaier'

import logging
import datetime


def format(resource_meta_datas, resource):
    score_extension = 0.0
    score_header = 0.0
    count_extension = 0.0
    count_header = 0.0
    for rmd in resource_meta_datas:
        if 'format' in rmd and rmd['format'] is not None:
            meta = rmd['format'].lower()

            # check file extensions
            if resource.url is not None:
                file_extension = analyze_resource_format.get_file_extension(resource.url)
                # assuming no file extension is longer than 10 characters
                if 0 < len(file_extension) < 10:
                    ext = file_extension[1:].lower()

                    if 'archive' in resource.parse_meta_data:
                        archive = resource.parse_meta_data['archive']
                        for f in archive:
                            count_extension += archive[f]
                            if f in analyze_resource_format.get_format_list(meta):
                                # TODO check if zip is in meta
                                score_extension += archive[f]
                    else:
                        count_extension += 1
                        if analyze_resource_format.get_format(ext) in analyze_resource_format.get_format_list(meta):
                            score_extension += 1

            # check mime type
            if 'header' in resource.crawl_meta_data and resource.crawl_meta_data['header'] is not None:
                header = resource.crawl_meta_data['header']
                cont_type = None
                if 'Content-Type' in header:
                    cont_type = header['Content-Type']
                elif 'content-type' in header:
                    cont_type = header['content-type']
                if cont_type is not None:
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

    if resource.crawl_meta_data['header'] is not None:
        if 'Content-Type' in resource.crawl_meta_data['header']:
            cont_type = resource.crawl_meta_data['header']['Content-Type']
        elif 'content-type' in resource.crawl_meta_data['header']:
            cont_type = resource.crawl_meta_data['header']['content-type']
        else:
            return result
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


def _try_get_language(param, param_name=None):
    try:
        if param_name == 'alpha2':
            return pycountry.languages.get(alpha2=param)
        elif param_name == 'bibliographic':
            return pycountry.languages.get(bibliographic=param)
        elif param_name == 'terminology':
            return pycountry.languages.get(terminology=param)
        elif param_name == 'common_name':
            return pycountry.languages.get(common_name=param)
        else:
            return pycountry.languages.get(name=param)
    except:
        return None


def language(resource_meta_datas, resource):
    result = {'content': {'count': 0, 'score': None},
              'header': {'count': 0, 'score': None}}

    score = 0.0
    count = 0.0
    for rmd in resource_meta_datas:
        if 'language' in rmd:
            if 'guessed_language' in resource.parse_meta_data:
                count += 1
                # guess is in 2-letter code
                guess = _try_get_language(resource.parse_meta_data['guessed_language'], 'alpha2')
                meta = rmd['language']
                if guess is not None and meta is not None:
                    meta = meta.lower()
                    if len(meta) == 2:
                        # iso_639_1
                        code = _try_get_language(meta, 'alpha2')
                    elif len(meta) == 3:
                        # iso_639_2
                        code = _try_get_language(meta, 'bibliographic')
                        if code is None:
                            code = _try_get_language(meta, 'terminology')
                    else:
                        code = _try_get_language(meta, 'name')
                        if code is None:
                            code = _try_get_language(meta, 'common_name')
                    if guess == code:
                        score += 1

    result['content']['count'] = count
    if count == 0:
        result['content']['score'] = None
    else:
        result['content']['score'] = score / count
    return result


def encoding(resource_meta_datas, resource):
    # check for header encoding info
    header_encoding = None
    if resource.crawl_meta_data['header'] is not None:
        if 'Content-Type' in resource.crawl_meta_data['header']:
            cont_type = resource.crawl_meta_data['header']['Content-Type']
        elif 'content-type' in resource.crawl_meta_data['header']:
            cont_type = resource.crawl_meta_data['header']['content-type']
        else:
            cont_type = None
        if cont_type:
            header = cont_type.split(';')
            if len(header) > 1:
                header_encoding = header[1]

    score_content = 0.0
    score_header = 0.0
    count_content = 0.0
    count_header = 0.0
    for rmd in resource_meta_datas:
        if 'characterset' in rmd:
            meta_encoding = rmd['characterset']
            if header_encoding:
                count_header += 1
                if meta_encoding is not None and len(meta_encoding) > 0:
                    if meta_encoding.lower() in header_encoding.lower():
                        score_header += 1
            elif 'guessed_encoding' in resource.parse_meta_data:
                guessed_encoding = resource.parse_meta_data['guessed_encoding']
                count_content += 1
                if guessed_encoding is not None and meta_encoding is not None:
                    if guessed_encoding.lower() == meta_encoding.lower():
                        score_content += 1

    result = {'content': {'count': count_content},
              'header': {'count': count_header}}
    if count_content > 0:
        result['content']['score'] = score_content / count_content
    else:
        result['content']['score'] = None
    if count_header > 0:
        result['header']['score'] = score_header / count_header
    else:
        result['header']['score'] = None
    return result


def size(resource_meta_datas, resource):
    result = {'content': {'count': 0, 'score': None},
              'header': {'count': 0, 'score': None}}

    if resource.crawl_meta_data['header'] is not None:
        if 'Content-Length' in resource.crawl_meta_data['header'] and \
                        resource.crawl_meta_data['header']['Content-Length'] is not None:
            header_field = float(resource.crawl_meta_data['header']['Content-Length'])
        elif 'content-length' in resource.crawl_meta_data['header'] and \
                        resource.crawl_meta_data['header']['content-length'] is not None:
            header_field = float(resource.crawl_meta_data['header']['content-length'])
        else:
            return result
    else:
        return result

    try:
        cont_length = float(header_field)
    except Exception:
        logger.error("(%s) Cannot read content length in header of resource: %s",
                     resource.url, header_field)
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


def compute_accuracy(dbm, portal, snapshot):
    # ############
    log_time_start = datetime.datetime.now()
    log_time_resource = datetime.timedelta()

    dt = datetime.datetime.fromtimestamp(snapshot)
    packages = dbm.getPackages(portal, snapshot)
    p_f = {}
    p_m = {}
    p_l = {}
    p_e = {}
    p_s = {}
    p_time_span = datetime.timedelta()
    for p in packages:
        package = Package(dict_string=p)
        urls = []
        f = {}
        m = {}
        l = {}
        e = {}
        s = {}
        max_time_span = datetime.timedelta()
        # ########
        log_time_resource_start = datetime.datetime.now()

        resources = dbm.getResourcesByPackageBeforeDateTimeDescending(portal, package, dt)
        for r_obj in resources:
            log_time_resource += datetime.datetime.now() - log_time_resource_start
            resource = Resource(dict_string=r_obj)
            # use newest resources (resources order descending by datetime)
            if resource.url not in urls:
                urls.append(resource.url)
                if package.content and type(package.content) is dict:
                    # calculate resource accuracy
                    package_resources = [r for r in package.content['resources'] if r['url'] == resource.url]
                    if len(package_resources) > 0:
                        try:
                            f[resource.url] = format(package_resources, resource)
                        except Exception as ex:
                            logger.error("(%s) during url %s: %s", portal.url, resource.url, ex.message)
                        try:
                            m[resource.url] = mime_type(package_resources, resource)
                        except Exception as ex:
                            logger.error("(%s) during url %s: %s", portal.url, resource.url, ex.message)
                        try:
                            l[resource.url] = language(package_resources, resource)
                        except Exception as ex:
                            logger.error("(%s) during url %s: %s", portal.url, resource.url, ex.message)
                        try:
                            e[resource.url] = encoding(package_resources, resource)
                        except Exception as ex:
                            logger.error("(%s) during url %s: %s", portal.url, resource.url, ex.message)
                        try:
                            s[resource.url] = size(package_resources, resource)
                        except Exception as ex:
                            logger.error("(%s) during url %s: %s", portal.url, resource.url, ex.message)

                        # calculate timespan of used resources
                        max_time_span = max(max_time_span, dt - resource.time)

                        # store accuracy per resource
                        resource.add_accuracy({
                            'format': f.get(resource.url, None),
                            'mime_type': m.get(resource.url, None),
                            'language': l.get(resource.url, None),
                            'encoding': e.get(resource.url, None),
                            'size': s.get(resource.url, None),
                            'time_span': (dt - resource.time).total_seconds()
                        })
                        dbm.storeResource(resource)

            # ######
            log_time_resource_start = datetime.datetime.now()

        log_time_resource += datetime.datetime.now() - log_time_resource_start

        # calcluate package accuracy
        p_f[package.id] = _compute_dict_accuracy(f)
        p_m[package.id] = _compute_dict_accuracy(m)
        p_l[package.id] = _compute_dict_accuracy(l)
        p_e[package.id] = _compute_dict_accuracy(e)
        p_s[package.id] = _compute_dict_accuracy(s)
        p_time_span = max(p_time_span, max_time_span)

        # store package accuracy in dataset_metrics
        metrics = dbm.getSingleDatasetMetrics(portal, package, snapshot)
        if metrics is not None:
            metrics.add_accuracy({
                'format': p_f[package.id],
                'mime_type': p_m[package.id],
                'language': p_l[package.id],
                'encoding': p_e[package.id],
                'size': p_s[package.id],
                'time_span': max_time_span.total_seconds()
            })
            dbm.storeDatasetMetrics(metrics)

    # calculate portal accuracy
    accuracy = {
        'format': _compute_dict_accuracy(p_f),
        'mime_type': _compute_dict_accuracy(p_m),
        'language': _compute_dict_accuracy(p_l),
        'encoding': _compute_dict_accuracy(p_e),
        'size': _compute_dict_accuracy(p_s),
        'time_span': p_time_span.total_seconds()
    }
    log_time_end = datetime.datetime.now()

    print 'total_acc: ' + str(log_time_end - log_time_start)
    print 'resource: ' + str(log_time_resource)

    return accuracy


def quality(portal, dbm, start, end):
    global logger
    logger = logging.getLogger(__name__)
    logger.info("(%s) Getting meta data statistics for the packages", portal.url)
    for snapshot in portal.snapshots:
        if (start < 0 or snapshot >= start) and (end < 0 or snapshot <= end):
            logger.info("Accuracy calculation for snapshot %s", snapshot)
            # get the portal meta data
            PMD = dbm.getPortalMetaData(portal.url, snapshot)

            accuracy = compute_accuracy(dbm, portal, snapshot)

            PMD.update_accuracy(accuracy)
            dbm.storePortalMetaData(PMD)

            logger.info("(%s) computed accuracy for snapshot %s", portal.url, snapshot)
