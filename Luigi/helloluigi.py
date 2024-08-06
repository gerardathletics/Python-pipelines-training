# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=line-too-long

# Create a luigi class that prints hello luigi
import luigi


# The class `HelloWorld` in Python defines a Luigi task that prints "hello" and writes "Hello world"
# to a local file named `hello_world.txt`.
class HelloWorld(luigi.Task):

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/hello_world.txt')

    def run(self):
        print("hello")
        with self.output().open('w') as f:
            f.write('Hello world')


# This Python class defines a Luigi task that prints "hello Luigi" and writes "Hello Luigi" to a local file.
class HelloLuigi(luigi.Task):
    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/hello_luigi.txt')

    def run(self):
        print("hello Luigi")
        with self.output().open('w') as f:
            f.write('Hello Luigi')


# This Luigi task class called GoodbyeLuigi requires HelloWorld and HelloLuigi tasks, outputs a local
# target file goodbye_luigi.txt, and writes "Goodbye Luigi" to the file when run.
class GoodbyeLuigi(luigi.Task):
    def requires(self):
        return [HelloWorld(), HelloLuigi()]

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/goodbye_luigi.txt')

    def run(self):
        print("goodbye Luigi")
        with self.output().open('w') as f:
            f.write('Goodbye Luigi')

# update the text in the file


class UpdateFile(luigi.Task):
    def requires(self):
        return [HelloWorld(), HelloLuigi(), GoodbyeLuigi()]

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/goodbye_luigi.txt')

    def run(self):
        print("goodbye Luigi")
        with self.output().open('w') as f:
            f.write('Goodbye Luigi, version 2')


if __name__ == '__main__':
    luigi.run(['UpdateFile', '--local-scheduler'])
