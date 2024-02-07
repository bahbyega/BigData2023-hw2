import string

from mrjob.job import MRStep, MRJob
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


class StaticAnalysis(MRJob):
    def map_init(self):
        import nltk
        nltk.download('punkt')
        nltk.download('stopwords')

    def map(self, _, line):
        parsed = (line.lower()).translate(str.maketrans('', '', string.punctuation)).split()[1:]

        phrase = ' '.join(parsed[1:])
        cleaned = [word for word in word_tokenize(phrase) if word not in set(stopwords.words('english'))]
        bigrams = [(cleaned[i], cleaned[i + 1]) for i in range(len(cleaned) - 1)]
        for bigram in bigrams:
            yield bigram, 1

    def reduce(self, bigram, values):
        yield bigram, sum(values)

    def map_best(self, bigram, count):
        yield None, (count, bigram)

    def reduce_best(self, _, bigrams_count):
        for count, bigram in sorted(bigrams_count, reverse=True)[:20]:
            yield bigram, count

    def steps(self):
        return [
            MRStep(mapper_init=self.map_init, mapper=self.map, reducer=self.reduce),
            MRStep(mapper=self.map_best, reducer=self.reduce_best)
        ]


if __name__ == "__main__":
    StaticAnalysis.run()