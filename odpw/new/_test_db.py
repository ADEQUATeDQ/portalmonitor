from odpw.new.db import DBClient

if __name__ == '__main__':

    db= DBClient(user='opwu', password='0pwu', host='localhost', port=1111, db='portalwatch')

    D= db.getDatasetData('c7668f5ec0a1c79c6996b41b3b331047')

    print D
    import pprint
    pprint.pprint(D.raw)