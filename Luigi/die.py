# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import random


class Die:

    def __init__(self, sides=6):
        self.sides = sides

    def roll(self):
        rolled = random.randint(1, self.sides)
        print(rolled)


die = Die(8)
die.roll()
