import zipfile
import rarfile
import tarfile

import os

__author__ = 'neumaier'


def get_file_extension(filename):
    fileName, fileExtension = os.path.splitext(filename)
    return fileExtension


def get_format(cell):
    if 'csv' in cell.lower(): return 'csv'
    if 'geojson' in cell.lower(): return 'geojson'
    if 'json' in cell.lower(): return 'json'
    if 'rss' in cell.lower(): return 'rss'
    if 'rdf' in cell.lower(): return 'rdf'
    if 'xlsx' in cell.lower(): return 'xlsx'
    if 'xls' in cell.lower(): return 'xls'
    if 'ods' in cell.lower(): return 'ods'
    if 'jpg' in cell.lower(): return 'jpeg'
    if 'jpeg' in cell.lower(): return 'jpeg'
    if 'pdf' in cell.lower(): return 'pdf'
    if 'txt' in cell.lower(): return 'txt'
    if 'text' in cell.lower(): return 'txt'
    return cell.lower()


def get_format_list(cell):
    entries = cell.split(' ')
    if len(entries) == 1:
        entries = cell.split(',')
    if len(entries) == 1:
        entries = cell.split(';')
    return [get_format(e) for e in entries]


def is_archive_extension(extension):
    if extension == 'zip':
        return True
    if extension == 'rar':
        return True
    if extension == 'tar':
        return True
    return False


def extract_formats_from_archive(file_content):
    if file_content is None:
        return

    formats = []
    if zipfile.is_zipfile(file_content):
        with zipfile.ZipFile(file_content) as zf:
            for info in zf.infolist():
                extension = get_file_extension(info.filename)
                if len(extension) > 0:
                    formats.append(get_format(extension[1:]))

    elif rarfile.is_rarfile(file_content):
        with rarfile.RarFile(file_content) as rf:
            for info in rf.infolist():
                extension = get_file_extension(info.filename)
                if len(extension) > 0:
                    formats.append(get_format(extension[1:]))

    elif tarfile.is_tarfile(file_content):
        with tarfile.TarFile(file_content) as tf:
            for info in tf.infolist():
                extension = get_file_extension(info.filename)
                if len(extension) > 0:
                    formats.append(get_format(extension[1:]))


    return formats