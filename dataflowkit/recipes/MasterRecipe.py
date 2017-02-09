from dataflowkit.recipes import BaseRecipe

class MasterRecipe(BaseRecipe):
    def __init__(self, m2s_channel, s2m_channels, s2m_sequence):
        self._m2s_channel = m2s_channel
        self._s2m_channels = s2m_channels
        self._s2m_sequence = s2m_sequence
    
    def execute(self, ins, outs):
        _ins = dict()
        for k, v in ins.items():
            _ins[k] = ins[k].load()
        
        _outs = dict()
        for k, v in outs.items():
            # _outs[k] = outs[k].load()
            _outs[k] = None
        
        s2m_index = self._s2m_sequence.get()
        self._m2s_channel.put((_ins, _outs, s2m_index))
        _outs = self._s2m_channels[s2m_index].get()
        self._s2m_sequence.put(s2m_index)
        
        for k, v in _outs.items():
            outs[k].save(v)