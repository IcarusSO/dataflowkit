import os
from dfm.datasets.BaseDataset import BaseDataset

class S3(BaseDataset):
    _data = None
    
    def __init__(self, conn, bucket, path):
        BaseDataset.__init__(self)
        self._conn = conn
        self._bucket = bucket
        self._path = path
        self._temporary_path = temporary_path
        
    def save(self, data):
        conn = self._conn
        bucket = self._bucket
        path = self._path
        
        f = tempfile.NamedTemporaryFile(delete=True, mode="wb")
        
        conn.upload(path, f, bucket)
        
        self._data = data
        
        
    def load(self):
        if self._data is None:
            self.reload()
        return self._data
           
    
    def reload(self):
        conn = self._conn
        path = self._path
        bucket = self._bucket
        
        bucket = conn.get_bucket(bucket)
        key = bucket.get_key(path)
        
        f = tempfile.NamedTemporaryFile(delete=True, mode="rb")
        
        key.get_contents_to_file(f)
        
        data = f.read()
        self._data = data
        f.close()

