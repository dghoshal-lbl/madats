import ConfigParser
import os
import sys

class Config():
    def __init__(self, config_file):
        self.__config_file__ = config_file
        
    '''
    def get(self, section):
        properties = ConfigParser.ConfigParser()
        properties.read(self.__properties_file__)
        items = dict(properties.items(section))
        return items
    '''

    def get(self, section, key):
        config = ConfigParser.ConfigParser()
        config.read(self.__config_file__)
        value = config.get(section, key)
        return value

