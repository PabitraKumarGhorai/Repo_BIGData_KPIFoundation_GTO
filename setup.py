from setuptools import setup, find_packages
from BigDataFoundationPackage.Scripts import *
import BigDataFoundationPackage

setup(
  name='BigDataFoundationPKG',
  version=BigDataFoundationPackage.__version__,
  author=BigDataFoundationPackage.__author__,
  url='https://community.cloud.databricks.com/',
  author_email='ghoraipabitrakumar@gmail.com',
  description='Framework to build wheel file for Databricks code',
  entry_points={"console_scripts":
  ['BigDataFoundationPackage = BigDataFoundationPackage.testwhl:main']
  
  },
  packages = ["BigDataFoundationPackage"],

)