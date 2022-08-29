from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class TotalVendas(MRJob):

    def steps(self):
         return[MRStep(mapper=self. mapper_get_estados, reducer = self.reducer_total_vendas)]

    def mapper_get_estados(self, _, line):

        try:
            (rowID,brand,model,year,body_type,state,price_dollar) = line.replace('"','').split(',')
            price_dollar = float(price_dollar)
        except Exception:
            pass
        else:
            yield (state, price_dollar)

    def reducer_total_vendas(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    TotalVendas.run()
