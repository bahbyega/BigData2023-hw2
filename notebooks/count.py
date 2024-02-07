from mrjob.job import MRStep, MRJob

class PhraseCount(MRJob):
    def map(self, _, line):
        parsed = (line.lower()).replace('"', '').split()[1:]
        yield parsed[0], 1

    def reduce(self, characters, values):
        yield characters, sum(values)

    def map_best(self, characters, count):
        yield None, (count, characters)

    def reduce_best(self, _, phrases_count):
        for count, characters in sorted(phrases_count, reverse=True)[:20]:
            yield characters, count

    def steps(self):
        return [
            MRStep(mapper=self.map, reducer=self.reduce),
            MRStep(mapper=self.map_best, reducer=self.reduce_best)
        ]


if __name__ == "__main__":
    PhraseCount.run()