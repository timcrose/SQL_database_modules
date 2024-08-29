from setuptools import find_packages
from setuptools import setup

setup(name = 'SQL_database_modules',
      description = 'Package for managing SQL databases.',
      author = 'T. Rose',
      url = 'https://github.com/timcrose/SQL_database_modules',
      packages = find_packages(),
      install_requires = ['pandas', 'SQLAlchemy']
     )
