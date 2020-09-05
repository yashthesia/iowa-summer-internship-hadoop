
from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]


    def mapper_get_ratings(self, _, line):
        data = line.split(',')
        yield data[1],(data[0]+","+data[12])

    def reducer_count_ratings(self, key, values):
        count = len(values);
        lname=[]
        lstar=[]
        for data in values:
            (name,stars) = data.split(',')
            lname.append(name)
            lstar.append(int(lstar))
        maxstar = max(lstar);
        maxname =""
        totalmax = 0
        for i in range(0,len(values)):
            if(maxstar is lstar[i]):
                maxname=lname[i]
                totalmax = totalmax+1
        yield key, str(count) +":"+str(maxname)+":"+str(maxstar)+":"+str(totalmax)

if __name__ == '__main__':
    RatingsBreakdown.run()


