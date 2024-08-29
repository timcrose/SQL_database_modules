"""
@author: timcr
"""
from sqlalchemy import inspect
from SQL_database_modules.database_connector import engine, sessionmaker
import pandas as pd


def query_db(desired_columns, query, join=[], order_by=None, verbose=False):
    '''
    desired_columns: list of Column objects or list of table
        List of Column objects e.g. [MarketHours.vwap, MarketHours.dollar_vol], or
        List of table classes e.g. [MarketHours]
        
    query: sqlalchemy.and_()
        Query that you would like to use to select data from the connected database.
        Use sqlalchemy.and_() to pass in one object.
        
    join: list of list of arguments to join()
        Each sub-list is a list of arguments to a join chain link
        e.g. join=[[Symbols]], join=[[Symbols, MarketHours.symbol_id == Symbols.symbol_id],
                                     [AnotherModel, MarketHours.another_id == AnotherModel.another_id]]
        
        The first example would execute 
session.query(*desired_columns).join(Symbols, MarketHours.symbol_id == Symbols.symbol_id)
        
        and the second would execute 
session.query(*desired_columns).join(Symbols, MarketHours.symbol_id == Symbols.symbol_id
).join(AnotherModel, MarketHours.another_id == AnotherModel.another_id)

    order_by: Column
        Column by which to order the rest of the columns in query_result by.
        
    verbose: bool
        True: print the head of the resulting dataframe
        False: do not print anything
        
    Returns
    -------
    df: pd.DataFrame
        Dataframe of the results of the query containing only the columns requested.

    Purpose
    -------
    Construct an intuitive, multi-filter, multi-relational query and formulate it into a DataFrame
    
    Notes
    -----
    1. filter() is more versatile than filter_by()
    2. query(MarketHours) returns all columns and has query.all() be a list of MarketHour objects
    3. query(MarketHours.vwap_pct, MarketHours.dollar_vol) to return just those two columns 
        and have query.all() be a list of sqlalchemy.util._collections.result objects
        result by sqlalchemy.orm.query.Query
    4. join accesses relational tables
    5. Sometimes the order of query_result is not the same as in the database
        so it is a good idea to order_by an index
    '''
    Session = sessionmaker(bind=engine)
    session = Session()
    query_result = session.query(*desired_columns)
    for join_call in join:
        query_result = query_result.join(*join_call)
    query_result = query_result.filter(query)
    if order_by is not None:
        query_result = query_result.order_by(order_by)
    df = pd.DataFrame(query_result)
    if verbose:
        print(df.head())
    session.close()
    return df


def create_tables(Base):
    '''
    Returns
    -------
    None.

    Purpose
    -------
    Create blank tables according to the schemas in the afore-defined classes 
    which inherit from Base.
    '''
    Base.metadata.create_all(engine)
    

def drop_tables(table_names=None, table_schemas=None):
    '''
    table_names: list of str or None
        Names of tables to drop from the database
        If None, use table_schemas to get table_names
        
    table_schemas: list of table schema class objects or None
        Each object should have a __tablename__ attribute.
        
    Returns
    -------
    None.

    Purpose
    -------
    Delete all tables from the database so that you can start over in case you
    made a mistake or want to change the schema.
    
    Notes
    -----
    1. table_names takes precedence over table_schemas if table_names is not None
    '''
    if table_names is None:
        table_names = [table_schema.__tablename__ for table_schema in table_schemas]
    for table_name in table_names:
        engine.execute("DROP TABLE " + table_name)
        

def get_existing_table_names(verbose=False):
    '''
    verbose: bool
        True: print info
        False: do not print info

    Returns
    -------
    table_names: list of str
        List of table names currently existent in the connected database

    Purpose
    -------
    Get the list of of table names currently existent in the connected database.
    '''
    # Create an inspector
    inspector = inspect(engine)
    
    # Get a list of table names
    table_names = inspector.get_table_names()
    if verbose:
        for table_name in table_names:
            print('table_name', table_name)
    return table_names
