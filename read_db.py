import sys
import logging
import os
import cx_Oracle
from config_reader import *
import argparse
from log_helper import samplelogging
class helper:
  def __init__(self,config):
    #get config
     self.config=config
     pass
     
def get_credentials(self):
  user_name=self.config.user_name
  pwd=self.config.pwd
  print user_name
  print pwd
