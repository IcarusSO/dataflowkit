from multiprocessing import Process, Queue, cpu_count
from dataflowkit import recipes as R
from dataflowkit import datasets as D
import threading
import multiprocessing
import numpy as np


def setup_s2m_queue(no_s2m_channels):
    s2m_index_queue = Queue()
    s2m_channels = [Queue() for _ in np.arange(no_s2m_channels)]
    for s2m_index in np.arange(no_s2m_channels):
        s2m_index_queue.put(s2m_index)
    return s2m_index_queue, s2m_channels


def create_slave_recipes(s2m_channels, m2s_channel, no_slaves, DummyRecipe, arg, lazy_arg):
    slave_recipes = [Process(target=SlaveRecipe, args=(m2s_channel, s2m_channels, DummyRecipe, arg, lazy_arg)) for _ in np.arange(no_slaves)]
    for slave_recipe in slave_recipes:
        slave_recipe.start()
    return m2s_channel


def SlaveRecipe(in_channel, out_channels, DummyRecipe, arg, lazy_arg):
    if lazy_arg is not None:
        arg = lazy_arg()
    dummy_recipe = DummyRecipe(*arg)
    
    while True:
        _ins, _outs, out_index = in_channel.get()
        
        ins = dict()
        for k, v in _ins.items():
            ins[k] = D.InMemory()
            ins[k].save(v)
            
        outs = dict()
        for k, v in _outs.items():
            outs[k] = D.InMemory()
            outs[k].save(v)
        
        dummy_recipe.execute(ins, outs)
        
        _outs = dict()
        for k, v in outs.items():
            _outs[k] = outs[k].load()
        
        out_channels[out_index].put(_outs)


class MasterRecipeFactory(object):
    _instance = None
    _lock = multiprocessing.Lock()
    _setup_lock = multiprocessing.Lock()
    
    @staticmethod
    def get_inatance(no_s2m_channels=None, no_m2s_channels=20):
        """
        Args:
            no_s2m_channels (int): Number of channels from slave to master, default is no. of cpu
        """
        if no_s2m_channels is None:
            no_s2m_channels = cpu_count()
            
        MasterRecipeFactory._lock.acquire()
        if MasterRecipeFactory._instance is None:
            MasterRecipeFactory._instance = MasterRecipeFactory(no_s2m_channels, no_m2s_channels)
        MasterRecipeFactory._lock.release()
        return MasterRecipeFactory._instance
    
    @staticmethod
    def wait(async_list):
        [_() for _ in async_list]
    
    def __init__(self, no_s2m_channels, no_m2s_channels):
        self._s2m = setup_s2m_queue(no_s2m_channels)
        self._m2s = setup_s2m_queue(no_m2s_channels)
        
        self._setup_dict = dict()
        
        self._setup_register_queue = Queue()
        self._setup_register_queue.put([])
    
    def __init__setup(self, DummyRecipe, m2s_channel, arg=[], lazy_arg=None, MasterRecipe=R.MasterRecipe, no_slaves=1):
        s2m_index_queue, s2m_channels = self._s2m
        
        m2s_channel = create_slave_recipes(s2m_channels, m2s_channel, no_slaves, DummyRecipe, arg, lazy_arg)

        setup_dict = {
            'm2s_channel': m2s_channel,
            'MasterRecipe': MasterRecipe
        }
        
        self._setup_dict[DummyRecipe] = setup_dict
        
        
    def create(self, DummyRecipe, arg=[], lazy_arg=None, MasterRecipe=R.MasterRecipe, no_slaves=1):
        """
        Args:
            DummyRecipe (BaseRecipe): 
            arg (sequence): arguments for DummyRecipe constructor
            lazy_arg (lambda): lambda to return arguments for DummyRecipe constructor, triger within the process
            MasterRecipe (BaseRecipe): the master recipe to communicate to the slave recipe, can be overriden
            no_slave (int): number of slave to serve the recipe
        """
        ##########################
        self._setup_lock.acquire()
        m2s_index_queue, m2s_channels = self._m2s
        setup_list = self._setup_register_queue.get()
        recipe_name = DummyRecipe.__name__
        if recipe_name in setup_list:
            index = setup_list.index(recipe_name)
        else:
            index = m2s_index_queue.get()
            setup_list.append(recipe_name)
            m2s_channel = m2s_channels[index]
            self.__init__setup(DummyRecipe, m2s_channel, arg, lazy_arg, MasterRecipe=MasterRecipe, no_slaves=no_slaves)
            
        self._setup_register_queue.put(setup_list)
        m2s_channel = m2s_channels[index]
        self._setup_lock.release()
        ##########################
        
        s2m_index_queue, s2m_channels = self._s2m

        return MasterRecipe(m2s_channel, s2m_channels, s2m_index_queue)
