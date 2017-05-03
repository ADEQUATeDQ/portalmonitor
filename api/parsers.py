from flask_restplus import reqparse
from werkzeug import datastructures

# file upload
from odpw.quality.dcat_analysers import dcat_analyser

post_param = reqparse.RequestParser()
post_param.add_argument('json_file',
                         type=datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='JSON file')

# url parameter
url_param = reqparse.RequestParser()
url_param.add_argument('url', required=True, help='URL of JSON metadata document')

qual_url = reqparse.RequestParser()
qual_url.add_argument('url', required=True, help='URL of JSON metadata document')
qual_post = reqparse.RequestParser()
qual_post.add_argument('json_file',
                         type=datastructures.FileStorage,
                         location='files',
                         required=True,
                         help='JSON file')
for q in [qual_post, qual_url]:
    q.add_argument('format', required=False, choices=['json-ld', 'json', 'csv'], default='json',
                  help='Output format')
    q.add_argument('metric', required=False, action='append', help='Filter by a specific metric', choices=[m.lower() for m in dcat_analyser().keys()])


for x in [url_param, post_param, qual_post, qual_url]:
    x.add_argument('software', required=True, choices=['CKAN', 'Socrata', 'OpenDataSoft'], default='CKAN',
                      help='Portal Software')
    x.add_argument('portal_url', required=False, help='Portal URL')
    x.add_argument('country', required=False, help='Country of data portal')
