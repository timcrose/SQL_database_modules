"""
@author: timcr
"""
from sqlalchemy import ForeignKey, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from python_utils import file_utils, time_utils
from database_connector import engine, sessionmaker
Base = declarative_base()

class Symbols(Base):
    '''
    Table of ticker symbols e.g. AAPL. integers are cheaper to store than strings so
    we'll create a relational database with symbols.
    '''
    __tablename__ = 'symbols'
    symbol_id = Column('symbol_id', Integer, primary_key=True, autoincrement=False)
    symbol = Column('symbol', String(10))

class PreMarketHours(Base):
    '''
    Each row is one second of a pre-market hours session
    
    vwap_pct: volume weighted average percent gain from the initial (9:30)
        price. (Averaged over the transactions that took place during that second). e.g. 1.01 means
        1% above the price at 9:30.
        
    dollar_vol: the sum of all stock price * volume for each transaction durinng that second.
    
    datetime_col: datetime of that second
    
    symbol_id: relates to the company ticker symbol.
    '''
    __tablename__ = 'pre_market_hours'
    pre_market_hours_id = Column(Integer, primary_key=True, autoincrement=False)
    vwap_pct = Column('vwap_pct', Float)
    dollar_vol = Column('dollar_vol', Integer)
    datetime_col = Column('datetime', DateTime(timezone=False))
    symbol_id = Column('symbol_id', Integer, ForeignKey('symbols.symbol_id'))

class MarketHours(Base):
    '''
    Each row is one second of a standard market hours session
    
    vwap_pct: volume weighted average percent gain from the initial (9:30)
        price. (Averaged over the transactions that took place during that second). e.g. 1.01 means
        1% above the price at 9:30.
        
    dollar_vol: the sum of all stock price * volume for each transaction durinng that second.
    
    datetime_col: datetime of that second
    
    symbol_id: relates to the company ticker symbol.
    '''
    __tablename__ = 'market_hours'
    market_hours_id = Column(Integer, primary_key=True, autoincrement=False)
    vwap_pct = Column('vwap_pct', Float)
    dollar_vol = Column('dollar_vol', Integer)
    datetime_col = Column('datetime', DateTime(timezone=False))
    symbol_id = Column('symbol_id', Integer, ForeignKey('symbols.symbol_id'))
    
class AfterMarketHours(Base):
    '''
    Each row is one second of a after-market hours session
    
    vwap_pct: volume weighted average percent gain from the initial (9:30)
        price. (Averaged over the transactions that took place during that second). e.g. 1.01 means
        1% above the price at 9:30.
        
    dollar_vol: the sum of all stock price * volume for each transaction durinng that second.
    
    datetime_col: datetime of that second
    
    symbol_id: relates to the company ticker symbol.
    '''
    __tablename__ = 'after_market_hours'
    after_market_hours_id = Column(Integer, primary_key=True, autoincrement=False)
    vwap_pct = Column('vwap_pct', Float)
    dollar_vol = Column('dollar_vol', Integer)
    datetime_col = Column('datetime', DateTime(timezone=False))
    symbol_id = Column('symbol_id', Integer, ForeignKey('symbols.symbol_id'))
    
    
def ingest_all_data(data_dir):
    '''
    data_dir: str
        Directory containing all .h5 stock data files.

    Returns
    -------
    None.

    Purpose
    -------
    Populate the afore-created tables with values written in the .h5 stock data files.
    Include the datetime column for easy querying.
    
    Notes
    -----
    1. Each file is named SYMBOL.h5 where SYMBOL is the ticker e.g. AAPL
    2. Each file is in a directory named like Ym e.g. 200908
    3. Each file contains dates metadata in format Ymd e.g. [20090829, 20090830]
    4. Each file contains a np array with each row being a second, starting at 7:00 EST
        and going until 20:00 EST.
    5. The columns of the data are vwap_pct: volume weighted average percent gain from the initial (9:30)
        price. (Averaged over the transactions that took place during that second). e.g. 1.01 means
        1% above the price at 9:30.
    6. dollar_vol means the sum of all stock price * volume for each transaction durinng that second.
    '''
    data_fpaths = file_utils.find(data_dir, '*.h5')[:2]
    syms = sorted(list(set([file_utils.fname_from_fpath(data_fpath) for data_fpath in data_fpaths])))
    Session = sessionmaker(bind=engine)
    session = Session()
    # Create the symbols table first so that the other tables can relate to it.
    sym_data = [Symbols(symbol_id=i, symbol=sym) for i,sym in enumerate(syms)]
    session.add_all(sym_data)
    session.commit()
    # IDs in the tables will be 0-based and so need to be incremented manually.
    pre_market_hours_id, market_hours_id, after_market_hours_id = 0, 0, 0
    for data_fpath in data_fpaths:
        # Add all market data for one file at a time so that you dont have to load the entire database into memory before committing. Also,
        # this is better than committing every single row one at a time.
        market_data = []
        sym = file_utils.fname_from_fpath(data_fpath)
        symbol_id = syms.index(sym)
        h5_data, attrs = file_utils.read_h5_file(data_fpath)
        for i in range(h5_data.shape[0]):
            # There are 6.5 hours in market hours, 2.5 hours in pre-market hours and 4 hours in after-market hours.
            # This makes 13 hours or 46800 seconds per day which means 46800 rows per day in the h5_data
            date_idx = int(i / 46800)
            date_str = str(attrs['dates'][date_idx])
            dt = time_utils.datetime.datetime.strptime(date_str, '%Y%m%d')
            # The pre-market hours session we say begins at 7:00 AM EST
            dt = time_utils.timedelta(dt, hours=7, seconds= i - date_idx * 46800)
            if dt.time() < time_utils.datetime.time(hour=9, minute=30):
                # market hours begins at 9:30 EST
                market_data.append(PreMarketHours(pre_market_hours_id=pre_market_hours_id, vwap_pct=h5_data[i][0], dollar_vol=h5_data[i][1], datetime_col=dt, symbol_id=symbol_id))
                pre_market_hours_id += 1
            elif dt.time() <= time_utils.datetime.time(hour=16):
                market_data.append(MarketHours(market_hours_id=market_hours_id, vwap_pct=h5_data[i][0], dollar_vol=h5_data[i][1], datetime_col=dt, symbol_id=symbol_id))
                market_hours_id += 1
            else:
                # market hours ends at 16:00 EST
                market_data.append(AfterMarketHours(after_market_hours_id=after_market_hours_id, vwap_pct=h5_data[i][0], dollar_vol=h5_data[i][1], datetime_col=dt, symbol_id=symbol_id))
                after_market_hours_id += 1
        # session.bulk_save_objects may not work for particular SQL database types and may also lead to relational issues.
        session.add_all(market_data)
    session.commit()
    session.close()
