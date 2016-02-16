'''
Created on Aug 10, 2015

@author: jumbrich
'''
from odpw.db.dbm import PostgressDBM
from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.core import DCATConverter
from odpw.db.models import Dataset
from odpw.utils.dataset_converter import dict_to_dcat
from odpw.analysers.dbm_handlers import DCATDistributionInserter
from odpw.analysers.count_analysers import DCATDistributionCount


if __name__ == '__main__':
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    for res in dbm.systemEvolution():
        print res