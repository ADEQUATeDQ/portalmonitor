#from distutils.core import setup
from setuptools import setup,find_packages
files = ["resources/*"]

print find_packages()
setup(
    name='odpw',
    version='0.1',
    packages = find_packages(),
    #package_data={'odpw':files},
    #package_data={
    #'static': 'odpw/web_rest/static/*',
    #'templates': 'odpw/web_rest/templates/*'},
    include_package_data = True,
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
        'psycopg2',
        'bokeh',
        'pybloom',
        'python-dateutil',
        'scrapy',
        'py-lru-cache',
        'queuelib',
        'rdflib',
        'rdflib-jsonld',
        'tldextract',
        'Twisted',
        'geomet',
        'reppy',
        'dictdiffer',
        'networkx',
        'Flask',
        'Flask-Cache',
        'flask-restplus'
    ],

)
