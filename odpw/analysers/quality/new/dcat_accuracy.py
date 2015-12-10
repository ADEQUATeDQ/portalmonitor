import mimetypes
from odpw.analysers import Analyser
from odpw.analysers.quality.analysers import analyze_resource_format
from odpw.utils.dcat_access import getDistributionFormats, getDistributionAccessURLs, getDistributionDownloadURLs, \
    getDistributionFormatWithURL, getDistributionMediaTypeWithURL, getDistributionSizeWithURL

__author__ = 'sebastian'



class AccuracyDCATAnalyser(Analyser):

    size_id = 'AcSi'
    format_id = 'AcFo'

    def __init__(self, resources):
        super(AccuracyDCATAnalyser, self).__init__()
        self.size_quality = None
        self.format_quality = None
        self.size_values = []
        self.format_values = []
        self.format_total = 0.0
        self.size_total = 0.0
        self.resources = resources

    def analyse_Dataset(self, dataset):
        urls = []
        urls += getDistributionAccessURLs(dataset)
        urls += getDistributionDownloadURLs(dataset)

        size_count = 0.0
        size_ds = 0.0
        format_count = 0.0
        format_ds = 0.0

        # iterate over resources of dataset
        for url in urls:
            meta_format = getDistributionFormatWithURL(dataset, url)
            meta_mime = getDistributionMediaTypeWithURL(dataset, url)
            meta_size = getDistributionSizeWithURL(dataset, url)

            # only consider urls which are stored in DB
            if url in self.resources:
                res = self.resources[url]
                res_mime = None if not res.mime or res.mime in ['missing', 'null', 'NULL', 'None', 'NA'] else res.mime
                res_size = None if not res.size or res.size in ['missing', 'null', 'NULL', 'None', 'NA'] else res.size

                # SIZE
                if meta_size and res_size:
                    size_ds += 1
                    if size(meta_size, res_size):
                        size_count += 1

                # FORMAT
                if (meta_format or meta_mime) and res_mime:
                    format_ds += 1
                    if format_calc(meta_format, meta_mime, res_mime):
                        format_count += 1

        # update total count
        if format_ds > 0:
            self.format_total += 1
            self.format_values.append(format_count/format_ds)
        if size_ds > 0:
            self.size_total += 1
            self.size_values.append(size_count/size_ds)

    def update_PortalMetaData(self, pmd):
        if not pmd.qa_stats:
            pmd.qa_stats = {}
        pmd.qa_stats[AccuracyDCATAnalyser.format_id] = self.format_quality
        pmd.qa_stats[AccuracyDCATAnalyser.size_id] = self.size_quality

    def done(self):
        self.format_quality = sum(self.format_values)/self.format_total if self.format_total > 0 else None
        self.size_quality = sum(self.size_values)/self.size_total if self.size_total > 0 else None


def format_calc(meta_format, meta_mime, res_mime):
    header_mime_type = res_mime.split(';')[0]
    guessed_extensions = mimetypes.guess_all_extensions(header_mime_type)

    if meta_format:
        meta_format = meta_format.lower()
        for ext in guessed_extensions:
            if analyze_resource_format.get_format(ext) in analyze_resource_format.get_format_list(meta_format):
                return True
    if meta_mime:
        meta_mime = meta_mime.split(';')[0]
        if header_mime_type == meta_mime:
                return True
    return False


def size(meta_size, res_size):
    try:
        cont_length = float(res_size)
        if cont_length <= 0:
            return False
    except Exception:
        return False

    try:
        # maybe ist a string with unit at end
        meta_string = meta_size.split(' ')
        meta = float(meta_string[0])
    except Exception:
        return False

    # try some units, give range, maybe its a string with unit at end...
    range = cont_length / 100.0
    if (cont_length - range) <= meta <= (cont_length + range):
        return True
    # check if it is in kb
    elif (cont_length - range) <= meta * 1000 <= (cont_length + range):
        return True
    # check if it is in kib
    elif (cont_length - range) <= meta * 1024 <= (cont_length + range):
        return True
    # check if it is in mb
    elif (cont_length - range) <= meta * 1000000 <= (cont_length + range):
        return True
    return False