from dataflowkit.datasets import BaseDataset
import pickle

class Local(BaseDataset):
    is_checkpoint = True
    
    def __init__(self, path):
        self._path = path
        self._data = None
    
    def save(self, data):
        with open(self._path, 'wb') as f:
            pickle.dump(data, f)
        self._data = data
    
    def load(self):
        if self._data is None:
            self.reload()
        return self._data
    
    def reload(self):
        try:
            with open(self._path, 'rb') as f:
                self._data = pickle.load(f)
        except:
            pass
        
        