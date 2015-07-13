#from distutils.core import setup
from setuptools import setup
files = ["resources/*"]
setup(
    name='odpw',
    version='0.1',
    packages=['odpw', 'odpw.db','odpw.quality','odpw.quality.analysers','odpw.reports','odpw.server','odpw.server.handler', 'odpw.utils' ],
    url='',
    license='',
    author='jumbrich',
    author_email='',
    description='',
    scripts = ["bin/odpw"],
    install_requires=[
        "requests",
        "faststat",
        'psycopg2',
        'structlog',
        'urlnorm'
    ],

)
