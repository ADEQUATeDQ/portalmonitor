__author__ = 'jumbrich'




import odpw.util
import odpw.ckanclient
import requests

import logging

class Portal:

    @classmethod
    def fromResult(cls, result):
        url=result['url']
        apiurl=result['apiurl']
        del result['url']
        del result['apiurl']
        return cls(url=url,
                   apiurl=apiurl,**result)

    @classmethod
    def newInstance(cls,url=None, apiurl=None, software='CKAN'):
        logger = logging.getLogger(__name__)

        ds=-1
        status=-1
        try:
            resp = ckanclient.package_get(apiurl)
            status=resp.status_code

            if resp.status_code != requests.codes.ok:
                logger.error("(%s) Something went wrong, no package list received, status=%s", apiurl, resp.status_code)
            else:
                package_list = resp.json()
                ds=len(package_list)
                logger.info('(%s) Found %s packages', apiurl, ds)
        except Exception as e:
            logger.warning("(%s) Error during fetching dataset information", apiurl)

        #TODO detect changefeed
        return cls(url=url,
                   apiurl=apiurl,
                   country=util.getCountry(url),
                   software=software,
                   datasets=ds,
                   resources=-1,
                   changefeed=False,
                   status=status
        )


    def __init__(self, url=None, apiurl=None, **kwargs):
        self.id=util.computeID(url)
        self.url=url
        self.apiurl=apiurl
        for key, value in kwargs.items():
            setattr(self, key, value)

