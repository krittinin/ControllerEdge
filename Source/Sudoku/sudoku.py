from collections import Iterator
from copy import deepcopy
from exactcover import ExactCover
from itertools import islice, product
from math import ceil, sqrt
from random import shuffle
from string import ascii_lowercase, ascii_uppercase

DIGITS = '123456789' + ascii_uppercase + ascii_lowercase


def make_grid(n):
    """Return a Sudoku grid of size n x n."""
    return [[-1] * n for _ in range(n)]


def puzzle_to_grid(puzzle):
    """Convert printed representation of a Sudoku puzzle into grid
    representation (a list of lists of numbers, or -1 if unknown).

    """
    puzzle = puzzle.split()
    grid = make_grid(len(puzzle))
    for y, row in enumerate(puzzle):
        for x, d in enumerate(row):
            grid[y][x] = DIGITS.find(d)
    return grid


def grid_to_puzzle(grid):
    """Convert grid representation of a Sudoku puzzle (a list of lists of
    numbers, or -1 if unknown) into printed representation.

    """
    return '\n'.join(''.join('.' if d == -1 else DIGITS[d] for d in row)
                     for row in grid)


class Sudoku(Iterator):
    """An iterator that yields the solutions to a Sudoku problem.

    The constructor takes three arguments:

    puzzle   The puzzle to solve, in the form of a string of n
             words, each word consisting of n characters, either a
             digit or a dot indicating a blank square.
             (Default: the blank puzzle.)
    n        The size of the puzzle. (Default: 9.)
    m        The width of the blocks. (Default: the square root of n,
             rounded up.)
    random   Generate solutions in random order? (Default: False.)

    For example:

        #>>> print(next(Sudoku('''...84...9
        ...                      ..1.....5
        ...                      8...2146.
        ...                      7.8....9.
        ...                      .........
        ...                      .5....3.1
        ...                      .2491...7
        ...                      9.....5..
        ...                      3...84...''')))
        632845179
        471369285
        895721463
        748153692
        163492758
        259678341
        524916837
        986237514
        317584926

    """

    def __init__(self, puzzle=None, n=9, m=None, random=False):
        if puzzle:
            puzzle = puzzle.split()
            self.n = n = len(puzzle)
            initial = self._encode_puzzle(puzzle)
        else:
            self.n = n
            initial = ()
        if m is None:
            m = int(ceil(sqrt(n)))
        assert (0 < n <= len(DIGITS))
        assert (n % m == 0)

        def constraints(choice):
            d, x, y = self._decode_choice(choice)
            block = m * (x // m) + y // (n // m)
            return [a + 4 * (b + n * c) for a, b, c in [
                (0, x, y),  # Any digit at x, y.
                (1, d, y),  # Digit d in row y.
                (2, d, x),  # Digit d in column x.
                (3, d, block),  # Digit d in block.
            ]]

        self.solver = ExactCover({i: constraints(i) for i in range(n ** 3)},
                                 initial, random)

    def __next__(self):
        return self._decode_solution(next(self.solver))

    next = __next__  # for compatibility with Python 2

    def _encode_choice(self, d, x, y):
        """Encode the placement of d at (x, y) as a choice."""
        n = self.n
        assert (0 <= d < n and 0 <= x < n and 0 <= y < n)
        return d + n * (x + n * y)

    def _decode_choice(self, choice):
        """Decode a choice into a (digit, x, y) tuple."""
        choice, digit = divmod(choice, self.n)
        y, x = divmod(choice, self.n)
        return digit, x, y

    def _encode_puzzle(self, puzzle):
        """Encode a Sudoku puzzle and yield initial choices."""
        for y, row in enumerate(puzzle):
            for x, d in enumerate(row):
                digit = DIGITS.find(d)
                if digit != -1:
                    yield self._encode_choice(digit, x, y)

    def _decode_solution(self, solution):
        """Decode a Sudoku solution and return it as a string."""
        grid = make_grid(self.n)
        for d, x, y in map(self._decode_choice, solution):
            grid[y][x] = d
        return grid_to_puzzle(grid)


class ImpossiblePuzzle(Exception): pass


class AmbiguousPuzzle(Exception): pass


def solve(puzzle, m=None):
    """Solve the Sudoku puzzle and return its unique solution. If the
    puzzle is impossible, raise ImpossiblePuzzle. If the puzzle has
    multiple solutions, raise AmbiguousPuzzle.

    Optional argument m is the width of the blocks (default: the
    square root of n, rounded up).

    For example:

       # >>> print(solve('''.6.5..9..
        ...                ..4.....6
        ...                .29..3..8
        ...                ....32..4
        ...                ..61.75..
        ...                1..95....
        ...                6..4..27.
        ...                2.....4..
        ...                ..7..6.8.'''))
        768514923
        314829756
        529763148
        975632814
        836147592
        142958637
        693481275
        281375469
        457296381
       # >>> print(solve('''...8.....
        ...                ..1.....5
        ...                8....1...
        ...                7.8....9.
        ...                ...1.....
        ...                .5....3.1
        ...                ....1...7
        ...                9.....5..
        ...                3........'''))
        ... # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
          ...
        ImpossiblePuzzle: no solutions

    """
    solutions = list(islice(Sudoku(puzzle, m=m), 2))
    if len(solutions) == 1:
        return solutions[0]
    elif len(solutions) == 0:
        raise ImpossiblePuzzle('no solutions')
    else:
        raise AmbiguousPuzzle('two or more solutions')


def make_puzzle(n=9, m=None):
    """Return a random nxn Sudoku puzzle with 180-degree rotational
    symmetry. The puzzle returned is minimal in the sense that no
    symmetric pair of givens can be removed without making the puzzle
    ambiguous.

    The optional arguments are n, the size of the puzzle (default: 9)
    and m, the width of the blocks (default: the square root of n,
    rounded up).

    """
    grid = puzzle_to_grid(next(Sudoku(n=n, m=m, random=True)))
    coords = list(divmod(i, n) for i in range((n ** 2 + 1) // 2))
    shuffle(coords)
    for i, j in coords:
        g = deepcopy(grid)
        g[i][j] = g[n - i - 1][n - j - 1] = -1
        try:
            solve(grid_to_puzzle(g))
            grid = g
        except AmbiguousPuzzle:
            pass
    return grid_to_puzzle(grid)
