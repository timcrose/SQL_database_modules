"""
@author: timcr
"""
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from python_utils import config_utils
# Get info from the user configuration file for security purposes.
config_fpath = 'sql.conf'
config = config_utils.Instruct(config_fpath)
config.get_config_dct()
username = config['main']['username']
password = config['main']['password']
database_name = config['main']['database_name']
hostname = config['main']['hostname']
sql_type = config['main']['sql_type']
db_url = sql_type + '://' + username + ':' + password + '@' + hostname + '/' + database_name
if not database_exists(db_url):
    # Instantiate the database.
    create_database(db_url)
# Connect to the database
engine = create_engine(db_url)


      
  
