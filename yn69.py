
from mrjob.job import MRJob
from mrjob.step import MRStep
from simhash import Simhash
import jieba
import subprocess
import lzo
class xiangsi(MRJob):
#    def mapper(self, _, line):
#        for word in line.split():
#            yield (word, 1)

#    def reducer(self, word, counts):
#        yield (word, sum(counts))

    def mapper(self, _, line):
        for word in line.split(' '):
            yield word, 1

#        data1 = "test"
#        output1 = subprocess.check_output("hdfs dfs -cat "+"\""+"/user/input/"+str(line)+"\"", shell=True)
#        data2 = lzo.decompress(output1)
#        words1 = jieba.lcut(data1, cut_all=True)
#        words2 = jieba.lcut(data2.decode(), cut_all=True)
#        dist=Simhash(words1).distance(Simhash(words2))
#        yield (dist,1)

    def reducer(self, word,counts):
        yield word, sum(counts)
if __name__ == '__main__':
    xiangsi.run()