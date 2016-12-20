from pymongo import MongoClient

class Connection():
  def __init__(self, host, port):
    self._host_ = host
    self._port_ = port

########################################################################
  def connect(self):
    self._client_ = MongoClient(self._host_, self._port_)
    return self._client_

########################################################################
  def close(self):
    self._client_.close()
 

