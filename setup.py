#from distutils.core import setup
from setuptools import setup
files = ["resources/*"]
setup(
    name='odpw',
    version='0.1',
    packages=['odpw', 'odpw.db','odpw.analysers','odpw.analysers.quality.analysers','odpw.analysers.quality','odpw.reporting','odpw.server','odpw.server.handler', 'odpw.utils','odpw.resources', 'odpw.analysers.quality.new' ],
    package_data={'odpw.resources':['*']},
    url='',
    license='',
    author='jumbrich',
    author_email='',
    description='',
    scripts = ["bin/odpw"],
    install_requires=[
        "requests",
        'structlog',
        'urlnorm',
        'sqlalchemy',
        'faststat',
        'ckanapi',
        'pandas',
        'numpy',
        'enum',
        'rarfile',
        'jinja2',
        'tornado',
        'pyyaml',
        'psycopg2'
        
    ],

)
