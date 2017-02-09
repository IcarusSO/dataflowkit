from dataflowkit.recipes import BaseRecipe
from dataflowkit.datasets import BaseDataset
from dataflowkit.utils.print_time import print_time, print_end


class Object(object):
    pass


class Node(object):
    def __init__(self, val, name):
        self.val = val
        self.name = name
        self.pars = set()
        self.childs = set()
        

class BaseGraph(Object):
    def _declare_recipes(self, r):
        raise Exception('_declare_recipes has to be overrided')
        
    def _declare_datasets(self, d):
        raise Exception('_declare_datasets has to be overrided')
        
    def _declare_graph(self, R, D):
        raise Exception('_declare_graph has to be overrided')
    
    def __init__(self):
        r = self._declare_recipes(Object())
        d = self._declare_datasets(Object())
        self._node_RD(r, d)
        self._graph = self._declare_graph(R=self.R, D=self.D)
        self._create_tree()
       
    def execute(self, desc=False):
        R = self.R
        D = self.D
        graph = self._graph
        
        # executed Nodes eN
        # pending Nodes pN
        eN = set()
        pN = set()
        
        for (recipe, ins, outs) in graph:
            pN.add(recipe)
            pN.update(ins)
            pN.update(outs)
        
        # find no par or all pars are executed
        i = 0
        while len(pN) != 0 and i < 10000:
            i += 1
            for n in pN:
                pars = n.pars
                if len(pars - eN) == 0:
                    pN.remove(n)
                    eN.add(n)
                    if isinstance(n.val, BaseRecipe):
                        self._execute_recipe(n, desc)
                    else:
                        self._execute_dataset(n, desc)
                        pass
                    break
                    
    
    def execute_related(self, nodes, desc=False):
        childs = set()
        pending_childs = set(nodes)
        i = 0
        while len(pending_childs) > 0 and i < 1000:
            child = pending_childs.pop()
            grand_childs = set(child.childs)
            new_pending_childs = grand_childs - childs
            pending_childs.update(new_pending_childs)
            childs.add(child)
            i += 1
            
        C = set(childs) # Set of Childs
        Q = list(childs) # Queue of Nodes pending to execute
        L = set() # Set of loaded/Ready Nodes
        i = 0
        while len(Q) > 0 and i < 10000:
            i += 1
            node = Q.pop()
            if isinstance(node.val, BaseDataset):
                if node in C:
                    pars = node.pars
                    unload_pars = set(pars) - L
                    if len(unload_pars) > 0:
                        Q.append(node)
                        for par in unload_pars:
                            if par in Q:
                                Q.remove(par)
                            Q.append(par)
                    else:
                        self._execute_dataset(node, desc)
                        L.add(node)
                else:  
                    if node.val.is_checkpoint is True:
                        self._execute_dataset(node, desc)
                        L.add(node)
                    else:
                        pars = node.pars
                        unload_pars = set(pars) - L
                        if len(unload_pars) > 0:
                            Q.append(node)
                            for par in unload_pars:
                                if par in Q:
                                    Q.remove(par)
                                Q.append(par)
                        else:
                            self._execute_dataset(node, desc)
                            L.add(node)
                        
            else:
                pars = node.pars
                unload_pars = set(pars) - L
                if len(unload_pars) > 0:
                    Q.append(node)
                    for par in unload_pars:
                        if par in Q:
                            Q.remove(par)
                        Q.append(par)
                else:
                    self._execute_recipe(node, desc)
                    L.add(node)
                    
                
    
    def _update_datasets(self, d):
        D = self.D
        for (k, v) in d.__dict__.items():
            getattr(D, k).val = v
        return d
    
    def get_d(self):
        D = self.D
        d = Object()
        for (k, v) in D.__dict__.items():
            setattr(d, k, v.val)
        return d
    
    def _execute_recipe(self, node, desc=False):
        if desc:
            print('-> ' + node.name)
            return
        # print_time('Time for executing ' + node.name)
        ins = {n.name: n.val for n in node.pars}
        outs = {n.name: n.val for n in node.childs}
        recipe = node.val
        recipe.execute(ins=ins, outs=outs)
        # print_end('Time for executing ' + node.name)
        
    def _execute_dataset(self, node, desc=False):
        if desc:
            print('-> ' + node.name)
            return
        
            
    def _create_tree(self):
        graph = self._graph
        for (recipe, ins, outs) in graph:
            for p in ins:
                recipe.pars.add(p)
                p.childs.add(recipe)
            for c in outs:
                recipe.childs.add(c)
                c.pars.add(recipe)
                
                
    def _node_RD(self, r, d):
        _R = Object()
        _D = Object()
        
        for (k, v) in r.__dict__.items():
            setattr(_R, k, Node(v, k))
        
        for (k, v) in d.__dict__.items():
            setattr(_D, k, Node(v, k))
                  
                    
        self.R = _R
        self.D = _D
        
        