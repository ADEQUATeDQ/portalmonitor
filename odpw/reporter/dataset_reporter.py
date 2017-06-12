

def report(datasetdata, datasetquality, software):

    reportInfo={}
    #existence
    if datasetquality.exda<1:
        reportInfo['ExDa'] = {'value': datasetquality.exda,
                              'explanation': 'Some of the creation and modification date fields for the dataset and resources are empty',
                              'improvement': 'Try to fill the modification and creation date fields'}
    if datasetquality.exri < 1:
        reportInfo['ExRi'] = {'value': datasetquality.exri,
                              'explanation': 'The dataset has no license information',
                              'improvement': 'Please add a license for your dataset'}

    if datasetquality.expr < 1:
        reportInfo['ExPr'] = {'value': datasetquality.expr,
                              'explanation': 'Information (size, format, mimetype,..) for preserving/archiving the dataset resource are missing',
                              'improvement': 'Try to add for each resouce the format, mime type, filesize and update frequency'}

    if not datasetquality.exac:
        reportInfo['ExAc']={'value':datasetquality.exac,
                            'explanation': 'Some of the resources do not have an access URL',
                            'improvement':'Provide an access URL for each of the resources'}
    if datasetquality.exdi < 1:
        reportInfo['ExDi'] = {'value': datasetquality.exdi,
                              'explanation': 'Some of the tilte, description and keyword fields are empty',
                              'improvement': 'Provide a title and description for your dataset and resources'}

    if datasetquality.exte < 1:
        reportInfo['ExTe'] = {'value': datasetquality.exte,
                              'explanation': 'Termporal information about the dataset',
                              'improvement': 'Provide some temporal context about the resources'}
        pass
    if datasetquality.exsp < 1:
        #reportInfo['ExSp'] = {'value': datasetquality.exsp,
        #                      'explanation': '',
        #                      'improvement': ''}
        pass
    if not datasetquality.exco:
        reportInfo['ExCo'] = {'value': datasetquality.exco,
                              'improvement': 'Add contact information to your dataset, such that consumers are able to contact you'}


    if not datasetquality.cocu:
        reportInfo['CoCU'] = {'value': datasetquality.cocu,
                              'explanation': 'The publisher or contact URL is not a syntacically valid URI',
                              'improvement': 'Provide a RFC 3986 conform URI for the contact or publisher'}
    if not datasetquality.coce:
        reportInfo['CoCE'] = {'value': datasetquality.coce,
                              'explanation': 'The publisher or contact URL is not a syntacically valid Email',
                              'improvement': 'Provide a RFC 5322 valid email address for the contact or publisher'}


    if datasetquality.coda < 1:
        reportInfo['CoDa'] = {'value': datasetquality.coda,
                              'explanation': 'Some of the creation and modification dates are not in a valid date format',
                              'improvement': 'Check hte date format for creation and modification fields'}

    #cofo = Column(Float)
    if datasetquality.cofo < 1:
        reportInfo['CoFo'] = {'value': datasetquality.cofo,
                              'explanation': 'Some of the specified mime types and file format are not registered with IANA (iana.org/)',
                              'improvement': 'Try to provide a file format and mime-type string which is registered with IANA'}

    #if not datasetqualitycoli = Column(Boolean)
    if datasetquality.coli < 1:
        reportInfo['CoLi'] = {'value': datasetquality.coli,
                              'explanation': 'The specified license could not mapped to the list provided by opendefinition.org',
                              'improvement': 'Try to provide a license ID/URL/Name which is registered with opendefinition.org. In case your license is not listed, please notify us or add them to the list.',
                              'extra':'http://licenses.opendefinition.org/licenses/groups/all.json'}
    #if not datasetqualitycoac = Column(Boolean)
    if datasetquality.coac < 1:
        reportInfo['CoAc'] = {'value': datasetquality.coac,
                              'explanation': 'The download or access URL is not a syntacically valid URI',
                              'improvement': 'Provide a RFC 3986 conform URI for the download or access URL of your resources'}

    if datasetquality.opfo < 1:
        reportInfo['OpFo'] = {'type':'warning',
                              'value': datasetquality.opfo,
                              'explanation': 'Some of the specified formats are not considerd open',
                              'improvement': 'Please inform us if you think we have an incomplete list or ignore this warning'}

    if datasetquality.opma < 1:
        reportInfo['OpMa'] = {'type':'warning',
                              'value': datasetquality.opma,
                              'explanation': 'Some of the specified formats are not to be machien readable',
                              'improvement': 'Please inform us if you think we have an incomplete list or ignore this warning'}

    if datasetquality.opli < 1:
        reportInfo['OpLi'] = {'type':'warning',
                              'value': datasetquality.opli,
                              'explanation': 'The specified license is not considerd to be open by the opendefinition.org',
                              'improvement': 'Please inform us if you think we have an incomplete list or ignore this warning'}

    return reportInfo
    ex = {}
    ex['ExAc'] = {'label': 'Access', 'color': '#311B92'
        , 'description': 'Does the meta data contain access information for the resources?'}
    ex['ExCo'] = {'label': 'Contact', 'color': '#4527A0'
        , 'description': 'Does the meta data contain information to contact the data provider or publisher?'}
    ex['ExDa'] = {'label': 'Date', 'color': '#512DA8'
        ,
                  'description': 'Does the meta data contain information about creation and modification date of metadata and resources respectively?'}
    ex['ExDi'] = {'label': 'Discovery', 'color': '#5E35B1'
        , 'description': 'Does the meta data contain information that can help to discover/search datasets?'}
    ex['ExPr'] = {'label': 'Preservation', 'color': '#673AB7'
        ,
                  'description': 'Does the meta data contain information about format, size or update frequency of the resources?'}
    ex['ExRi'] = {'label': 'Rights', 'color': '#7E57C2'
        , 'description': 'Does the meta data contain information about the license of the dataset or resource.?'}
    ex['ExSp'] = {'label': 'Spatial', 'color': '#9575CD'
        , 'description': 'Does the meta data contain spatial information?'}
    ex['ExTe'] = {'label': 'Temporal', 'color': '#B39DDB'
        , 'description': 'Does the meta data contain temporal information?'}
    existence = {'dimension': 'Existence', 'metrics': ex, 'color': '#B39DDB'}

    ac = {}
    ac['AcFo'] = {'label': 'Format', 'color': '#00838F'
        , 'description': 'Does the meta data contain information that can help to discover/search datasets?'}
    ac['AcSi'] = {'label': 'Size', 'color': '#0097A7'
        , 'description': 'Does the meta data contain information that can help to discover/search datasets?'}
    accuracy = {'dimension': 'Accurracy', 'metrics': ac, 'color': '#0097A7'}

    co = {}
    co['CoAc'] = {'label': 'AccessURL', 'color': '#388E3C'
        , 'description': 'Are the available values of access properties valid HTTP URLs?'}
    co['CoCE'] = {'label': 'ContactEmail', 'color': '#1B5E20'
        , 'description': 'Are the available values of contact properties valid emails?'}

    co['CoCU'] = {'label': 'ContactURL', 'color': '#43A047'
        , 'description': 'Are the available values of contact properties valid HTTP URLs?'}
    co['CoDa'] = {'label': 'DateFormat', 'color': '#66BB6A'
        , 'description': 'Is date information specified in a valid date format?'}
    co['CoFo'] = {'label': 'FileFormat', 'color': '#A5D6A7'
        , 'description': 'Is the specified file format or media type registered by IANA?'}
    co['CoLi'] = {'label': 'License', 'color': '#C8E6C9'
        ,
                  'description': 'Can the license be mapped to the list of licenses reviewed by <a href="http://opendefinition.org/">opendefinition.org</a>?'}
    conformance = {'dimension': 'Conformance', 'metrics': co, 'color': '#C8E6C9'}

    op = {}
    op['OpFo'] = {'label': 'Format Openness', 'color': '#F4511E'
        , 'description': 'Is the file format based on an open standard?'}
    op['OpLi'] = {'label': 'License Openneness', 'color': '#FF8A65'
        , 'description': 's the used license conform to the open definition?'}
    op['OpMa'] = {'label': 'Format machine readability', 'color': '#E64A19'
        , 'description': 'Can the file format be considered as machine readable?'}
    opendata = {'dimension': 'Open Data', 'metrics': op, 'color': '#E64A19'}

    re = {}
    re['ReDa'] = {'label': 'Datasets', 'color': '#FF9800'}
    re['ReRe'] = {'label': 'Resources', 'color': '#FFA726'}
    retrievability = {'dimension': 'Retrievability', 'metrics': re, 'color': '#FFA726'}

    qa = [existence, conformance, opendata]  # , retrievability, accuracy]