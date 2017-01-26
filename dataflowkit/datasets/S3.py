import tempfile
import pickle
from boto import s3
from dataflowkit.datasets.BaseDataset import BaseDataset

class S3(BaseDataset):
    _data = None
    is_checkpoint = True
    
    def __init__(self, bucket, path):
        BaseDataset.__init__(self)
        self._bucket = bucket
        self._path = path
        
    def save(self, data):
        bucket = self._bucket
        path = self._path
        
        key = s3.key.Key(bucket, path)
        
        f = tempfile.NamedTemporaryFile(delete=True, mode="wb")
        pickle.dump(data, f)
        
        with open(f.name, 'rb') as f:
            key.set_contents_from_string(f.read())
        
        f.close()
        self._data = data
        
        
    def load(self):
        if self._data is None:
            self.reload()
        return self._data
           
    
    def reload(self):
        bucket = self._bucket
        path = self._path
        
        key = s3.key.Key(bucket, path)
        
        f = tempfile.NamedTemporaryFile(delete=True, mode="wb")
        key.get_contents_to_file(f)
        with open(f.name, 'rb') as f:
            data = pickle.load(f)
        f.close()

        self._data = data
