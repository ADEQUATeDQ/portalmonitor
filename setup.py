#from distutils.core import setup
from setuptools import setup,find_packages
from pip.req import parse_requirements
files = ["resources/*"]
install_reqs = parse_requirements("requirements.txt", session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

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
    install_requires=reqs

)
