from odpw.analysers.core import ElementCountAnalyser

__author__ = 'sebastian'


class AllTypeDCAT(ElementCountAnalyser):
    def __init__(self, accessFunct, type_set):
        super(AllTypeDCAT, self).__init__()
        self.af=accessFunct
        self.type_set = type_set

    def analyse_Dataset(self, dataset):
        values = self.af(dataset)
        for v in values:
            # resource level
            if v not in self.type_set:
                self.analyse_generic(False)
                return False
        self.analyse_generic(True)
        return True


class ContainsTypeDCAT(ElementCountAnalyser):
    def __init__(self, accessFunct, type_set):
        super(ContainsTypeDCAT, self).__init__()
        self.af=accessFunct
        self.type_set = type_set

    def analyse_Dataset(self, dataset):
        values = self.af(dataset)
        for v in values:
            # resource level
            if v in self.type_set:
                self.analyse_generic(True)
                return True
        self.analyse_generic(False)
        return False


class OfTypeDCAT(ElementCountAnalyser):
    def __init__(self, accessFunct, type_set):
        super(OfTypeDCAT, self).__init__()
        self.af=accessFunct
        self.type_set = type_set

    def analyse_Dataset(self, dataset):
        values = self.af(dataset)
        if len(values) == 0:
            self.analyse_generic('no values')
        for v in values:
            # resource level
            if v in self.type_set:
                self.analyse_generic(True)
            else:
                self.analyse_generic(False)
        return values
