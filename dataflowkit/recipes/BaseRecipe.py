class BaseRecipe(object):
    def execute(self, ins, outs):
        raise NotImplementedError("Recipe::execute(self, ins, outs) has not been implemented")
