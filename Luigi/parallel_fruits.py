# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=line-too-long

import pandas as pd
import luigi


class GenerateWords(luigi.Task):
    fruit = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f'{self.fruit}_words.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write(f'{self.fruit}\n')


class CountLetters(luigi.Task):
    fruit = luigi.Parameter()

    def requires(self):
        return GenerateWords(fruit=self.fruit)

    def output(self):
        return luigi.LocalTarget(f'{self.fruit}_letter_counts.txt')

    def run(self):
        with self.input().open('r') as infile:
            words = infile.read().splitlines()
        with self.output().open('w') as outfile:
            for word in words:
                outfile.write(f'{word}|{len(word)}\n')


class SortCounts(luigi.Task):
    fruit = luigi.Parameter()

    def requires(self):
        return CountLetters(fruit=self.fruit)

    def output(self):
        return luigi.LocalTarget(f'{self.fruit}_sorted_letter_counts.txt')

    def run(self):
        with self.input().open('r') as infile:
            df = pd.read_csv(infile, sep="|", header=None,
                             names=['word', 'count'])
            df = df.sort_values(by='count', ascending=True)
        with self.output().open('w') as outfile:
            for _, row in df.iterrows():
                outfile.write(f'{row["word"]} | {row["count"]}\n')


class RunAllFruits(luigi.WrapperTask):
    def requires(self):
        fruits = ['apple', 'banana', 'grapefruit',
                  'grape', 'watermelon', 'lime']
        for fruit in fruits:
            yield SortCounts(fruit=fruit)


if __name__ == '__main__':
    luigi.run(['RunAllFruits', '--local-scheduler'])
