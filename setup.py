#from distutils.core import setup
from setuptools import setup,find_packages
files = ["resources/*"]
server_files=[  'static/css/*','static/js/*','static/data/*',
                'static/fonts/EOT/*','static/fonts/OTF/*','static/fonts/TTF/*','static/fonts/*.*',
                'static/fonts/WOFF/OTF/*','static/fonts/WOFF/TTF/*','static/fonts/WOFF2/OTF/*','static/fonts/WOFF2/TTF/*',
                'static/images/*','static/vega_spec/*','templates/*']

p= find_packages()
p.append('odpw.resources')
print p
setup(
    name='odpw',
    version='0.1',
    packages = p,
    #package_dir={'':'odpw'},
    #packages=[  'odpw', 
    #            'odpw.db',
    #            'odpw.analysers','odpw.analysers.quality.analysers','odpw.analysers.quality','odpw.analysers.quality.new',
    #            'odpw.reporting',
    #            'odpw.server','odpw.server.handler',
    #             'odpw.utils','odpw.resources',  ],
    package_data={'odpw.resources':['iana/*'], 'odpw.server':server_files},
    #package_data = {'odpw' : files },
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
        'python-nvd3',
        'scrapy',
        'py-lru-cache',
        'queuelib',
        'rdflib',
        'rdflib-jsonld',
        'tldextract',
        'Twisted',
        'matplotlib',
    ],

)
