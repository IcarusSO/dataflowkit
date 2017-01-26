from dataflowkit.datasets.BaseDataset import BaseDataset
import pandas as pd

class SQLAlchemy(BaseDataset):
    def __init__(self, session, model, where=None, columns=None):
        self._session = session
        self._model = model
        self._where = where
        self._columns = columns
    
    def save(self, data):
        session = self._session
        model = self._model
        
        self._remove_past_data()
        rows = data.to_dict(orient="row")
        
        for row in rows:
            attrs = list(row)
            for attr in attrs:
                row[attr] = None if row[attr] is None else str(row[attr])
        
        objs = [model(**row) for row in rows]
        session.add_all(objs)
        
        session.flush()
        
        ids = [obj.id for obj in objs]
        data['id'] = ids
        self._data = data
        
    def load(self):
        if self._data is None:
            self.reload()
        return self._data

    def reload(self):
        session = self._session
        model = self._model
        where = self._where
        columns = self._columns

        if columns is None:
            return

        rows = session.query(*[getattr(model, column) for column in columns])\
            .filter_by(**where).all()

        data = pd.DataFrame(rows, columns=columns)
        self._data = data

    
    def _remove_past_data(self):
        session = self._session
        where = self._where
        if where is None:
            return
        
        model = self._model
        objs = session.query(model).filter_by(**where).all()
        for obj in objs:
            session.delete(obj)
            session.flush()