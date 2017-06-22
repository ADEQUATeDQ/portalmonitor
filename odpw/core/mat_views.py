import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import Query
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy.ext import compiler

from odpw.core.model import Base


class CreateView(DDLElement):
    def __init__(self, name, query):
        self.name = name
        self.selectable = query

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name

from psycopg2.extensions import adapt as sqlescape
def compile_query(query):
    dialect = query.session.bind.dialect
    statement = query.statement
    comp = sqlalchemy.sql.compiler.SQLCompiler(dialect, statement)
    comp.compile()
    enc = dialect.encoding
    params = {}
    for k,v in comp.params.iteritems():
        if isinstance(v, unicode):
            v = v.encode(enc)
        params[k] = sqlescape(v)
    return (comp.string.encode(enc) % params).decode(enc)

@compiler.compiles(CreateView)
def compile(element, compiler, **kw):
    return "CREATE MATERIALIZED VIEW %s AS %s" % (element.name, compile_query(element.selectable))

@compiler.compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP MATERIALIZED VIEW IF EXISTS %s" % (element.name)

def view(name, metadata, selectable):
    t = table(name)

    orig=selectable
    if isinstance(selectable, Query):
        selectable = selectable.subquery()

    for c in selectable.c:
        c._make_proxy(t)

    CreateView(name, orig).execute_at('after-create', metadata)
    DropView(name).execute_at('before-drop', metadata)
    return t


def withView(query, viewName, session, dbc, metadata=Base.metadata):
    ## check first if the view exist
    viewTable=None
    try:
        viewTable = Table(viewName, metadata, autoload=True, autoload_with=dbc.dbm.engine)
        print "Using view data"
        return session.query(viewTable)
    except:
        return query

def createView(query, viewName, session):
    c = CreateView(viewName, query)
    session.execute(c)
    session.commit()

    #tt=view(viewName, metadata, query)
    #print 'tt',tt.compile()
    #metadata.create_all(dbc.dbm.engine, checkfirst=True)  # create if not existing

