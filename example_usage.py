"""
@author: timcr
"""
from table_schema import Symbols, MarketHours, ingest_all_data, Base
from database_manager import query_db, drop_tables, create_tables
import datetime
from sqlalchemy import and_


def example_create_tables():
    '''
    Returns
    -------
    None.

    Purpose
    -------
    This function will create a database called database_name 
    (as specified in your sql.conf) (if does not already exist) and blank 
    tables as specified in your table_schema.py into your SQL server.
    '''
    create_tables(Base)


def example_ingest_all_data():
    '''
    Returns
    -------
    None.

    Purpose
    -------
    Ingest all data into the database specified in your sql.conf according
    to the ingestion methodology specified in table_schema.py
    
    Notes
    -----
    1. You must call create_tables before this function
    '''
    ingest_all_data('h5_datasets')
    
def example_query():
    '''
    Returns
    -------
    None.

    Purpose
    -------
    Query the pre-existing database specified in your sql.conf
    '''
    query_result_df = query_db(
desired_columns=[MarketHours.vwap_pct, MarketHours.dollar_vol, MarketHours.datetime_col],
query=and_(
MarketHours.vwap_pct >= 0.964, MarketHours.vwap_pct <= 0.965, 
MarketHours.dollar_vol > 0,
MarketHours.datetime_col > datetime.datetime(2009,10,30,15,45,20),
Symbols.symbol == 'AA'
),
join=[[Symbols]],  
order_by=MarketHours.market_hours_id, 
verbose=True)


def example_drop_tables():
    '''
    Returns
    -------
    None.

    Purpose
    -------
    Delete the tables with names specified here from the database specified
    in your sql.conf
    '''
    drop_tables(['Symbols', 'PreMarketHours', 'MarketHours', 'AfterMarketHours'])
    
    
if __name__ == '__main__':
    example_create_tables()
    example_ingest_all_data()
    example_query()
    example_drop_tables()
