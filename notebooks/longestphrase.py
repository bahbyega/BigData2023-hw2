from mrjob.job import MRStep, MRJob

class LongestPhrase(MRJob):
    def map(self, _, line):
        parsed = (line.lower()).replace('"', '').split()[1:]
        phrase = ' '.join(parsed[1:])

        yield parsed[0], (len(phrase), phrase)

    def reduce(self, characters, phrase_n_len):
        l, ph = max(phrase_n_len, key=lambda c: c[0])
        yield None, (characters, l, ph)

    def reduce_best(self, _, character_phrases):
        for character, _, phrase in sorted(character_phrases, key=lambda c: c[1], reverse=True):
            yield character, [phrase]

    def steps(self):
        return [
            MRStep(mapper=self.map, reducer=self.reduce),
            MRStep(reducer=self.reduce_best)
        ]


if __name__ == "__main__":
    LongestPhrase.run()