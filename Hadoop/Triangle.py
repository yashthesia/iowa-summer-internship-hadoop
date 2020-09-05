from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [

            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
            MRStep(reducer=self.reducer_3),
            MRStep(mapper=self.mapper_4,
                   reducer=self.reducer_4),
            MRStep(mapper=self.mapper_5,
                   reducer=self.reducer_5)

        ]

    def mapper_1(self, _, line):
        l = line.split('\t')
        x = int(l[0])
        y = int(l[1])
        if x < y :
            yield str(x)+","+str(y),1
        else:
            yield str(y)+","+str(x),1

    def reducer_1(self, key, values):

        yield key , sum(values)
    def reducer_2(self,key,values):
        l = str(key).split(',')
        x = int(l[0])
        y = int(l[1])
        yield x,y

    def reducer_3(self,key,values):
        l = []
        for _ in values:
            l.append(_)
        yield key,l
    def mapper_4(self,key,values):
        for i in values:
            yield str(key)+","+str(i),"nn"
        for i in values:
            for j in values:
                if i < j :
                    yield str(i)+","+str(j),"n"

    def reducer_4(self,key,values):
        l = []
        for _ in values:
            l.append(_)
        yield key,l
    def mapper_5(self,key,values):
        canMake = 0
        count = 0
        for _ in values:
            if str(_) == "nn":
                canMake =1
            else :
                count = count + 1
        if canMake == 1:
            yield "triangle count",count
    def reducer_5(self,key,values):

        yield key,sum(values)


if __name__ == '__main__':
    RatingsBreakdown.run()