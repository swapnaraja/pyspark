import sys
import commands
from ConfigParser import SafeConfigParser

class ReadConfig:
  config=SafeConfigParser()
  def __init__(self,filename):
    self.read_config(filename)
  def read_config(self,file_name):
    self.config.read(file_name)
    
    #reading configs
    self.username=self.config.get('credentials','user_name')
    self.pwd=self.config.get('credentials','passwrd')
    self.server_name=self.config.get('server_info','server_name')
    
    #sample config file
    [credentials]
    user_name='Sample'
    passwrd='*****'
    
    [server_info]
    server_name='abcd'
