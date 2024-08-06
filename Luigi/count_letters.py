# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=line-too-long

import luigi


class GenerateWords(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/words.txt')

    def run(self):
        # write a dummy list of words to output file�
        words = ['apple', 'banana', 'grapefruit',
                 'Gerard', 'Ordina', 'SopraSteria']
        with self.output().open('w') as f:
            for word in words:
                f.write('{word}\n'.format(word=word))


class CountLetters(luigi.Task):
    def requires(self):
        return GenerateWords()

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/letter_counts.txt')

    def run(self):
        # read in file as list�
        with self.input().open('r') as infile:
            words = infile.read().splitlines()
            # write each word to output file with letter count�
            with self.output().open('w') as outfile:
                for word in words:
                    outfile.write('{word} | {letter_count}\n'.format(
                        word=word, letter_count=len(word)))


# Sort the words in the file by number of letters
class SortWords(luigi.Task):
    def requires(self):
        return CountLetters()

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/sorted_words.txt')

    def run(self):
        """
        This function reads words from an input file, sorts them based on the number of letters in each word, and writes the sorted words to an output file.
        """
        with self.input().open('r') as infile:
            words = infile.read().splitlines()
            # sort words by number of letters
            words.sort(key=lambda x: int(x.split(' | ')[1]))
            # write sorted words to output file
            with self.output().open('w') as outfile:
                for word in words:
                    outfile.write('{word}\n'.format(word=word))


if __name__ == '__main__':
    luigi.run(['SortWords', '--local-scheduler'])
