#from distutils.core import setup
from setuptools import setup,find_packages
files = ["resources/*"]

from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements("requirements.txt", session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]


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
    install_requires=reqs,
    # install_requires=[
    #     "requests",
    #     'structlog',
    #     'urlnorm',
    #     'sqlalchemy',
    #     'faststat',
    #     'ckanapi',
    #     'pandas',
    #     'numpy',
    #     'enum',
    #     'rarfile',
    #     'jinja2',
    #     'tornado',
    #     'pyyaml',
    #     'psycopg2',
    #     'bokeh',
    #     'pybloom',
    #     'python-dateutil',
    #     'scrapy',
    #     'py-lru-cache',
    #     'queuelib',
    #     'rdflib',
    #     'rdflib-jsonld',
    #     'tldextract',
    #     'Twisted',
    #     'geomet',
    #     'reppy',
    #     'dictdiffer',
    #     'networkx',
    #     'Flask',
    #     'Flask-Cache',
    #     'flask-restplus',
    #     'rdflib'
    # ],

)
