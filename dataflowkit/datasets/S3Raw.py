import tempfile
from boto import s3
import os
from dataflowkit.datasets.S3 import S3

class S3Raw(S3):
    is_checkpoint = True
        
    def save(self, data):
        bucket = self._bucket
        path = self._path
        
        key = s3.key.Key(bucket, path)
        
        f = tempfile.NamedTemporaryFile(delete=False, mode="wb")
        f.write(data)
        
        with open(f.name, 'rb') as f:
            key.set_contents_from_string(f.read())
        
        f.close()
        self._data = data
        
    
    def reload(self):
        bucket = self._bucket
        path = self._path
        
        key = s3.key.Key(bucket, path)
        
        f = tempfile.NamedTemporaryFile(delete=False, mode="wb")
        key.get_contents_to_file(f)
        f.close()
        with open(f.name, 'rb') as f:
            data = f.read()
        os.remove(f.name)

        self._data = data
