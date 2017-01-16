class BaseRecipe(object):
    def execute(self, ins, outs):
        raise NotImplementedError("Recipe::execute(ins, outs) has not been implemented")
