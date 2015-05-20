from distutils.core import setup

files = ["resources/*"]
setup(
    name='odpw',
    version='',
    packages=['odpw', 'odpw.db' ],
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
