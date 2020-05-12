import os
import io
import sys
import time
import shutil
import pkgutil
from pathlib import Path
from functools import wraps
from itertools import chain, islice

from dataloader import logging

logger = logging.getLogger(__name__)


def iter_chunks(iterator, size):
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def time_stat(func):
    """ Statistics the time cost of a
    function executing using the time module.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        s = time.time()
        ret = func(*args, **kwargs)
        c = round(time.time() - s, 2)
        logger.info(
            "[STAT %f] %s.%s cost %.2f secs.",
            time.time(), func.__module__, func.__name__, c
        )
        return ret
    return wrapper


def get_root_path(import_name):
    """ Copy from flask source src/flask/helpers.py:L764 """
    mod = sys.modules.get(import_name)
    if mod is not None and hasattr(mod, "__file__"):
        return os.path.dirname(os.path.abspath(mod.__file__))

    loader = pkgutil.get_loader(import_name)

    if loader is None or import_name == "__main__":
        return os.getcwd()

    if hasattr(loader, "get_filename"):
        filepath = loader.get_filename(import_name)
    else:
        __import__(import_name)
        mod = sys.modules[import_name]
        filepath = getattr(mod, "__file__", None)

        if filepath is None:
            raise RuntimeError(
                "No root path can be found for the provided module"
                f" {import_name!r}. This can happen because the module"
                " came from an import hook that does not provide file"
                " name information or because it's a namespace package."
                " In this case the root path needs to be explicitly"
                " provided."
            )

    return os.path.dirname(os.path.abspath(filepath))


def make_dir(root_path, sub_path, remove_if_exists=False):
    """ Create directory inclusively """
    full_path = os.path.join(root_path, sub_path)
    
    posix_path = Path(full_path)
    try:
        if remove_if_exists:
            shutil.rmtree(posix_path)
    except:
        pass

    posix_path.mkdir(parents=True, exist_ok=True)

    return full_path


def full_pyfile_name(absolute_path, filename):
    """ Generate a full py source filename """
    return os.path.join(absolute_path, filename + ".py")


def to_camel_case(snake_str):
    """ Convert snake string to camel string: snake_str -> SnakeStr """
    components = snake_str.split('_')
    return ''.join(x.title() for x in components)


def clean_csv_value(value):
    """ clean csv format value """
    if value is None:
        return r'\N'

    return str(value).replace('\n', '\\n')


class FileUtil(object):
    """ A file util for writing source code files """
    def __init__(self, filename):
        self.fp = open(filename, 'w+', buffering=10)

    def blankline(self, count=1):
        for i in range(count):
            self.fp.write("\n")
        self.fp.flush()

    def writeline(self, line, indent=0):
        blank = ''
        for i in range(indent):
            blank += ' '
        self.fp.write(blank + line)
        self.fp.write("\n")

    def saveall(self):
        self.fp.flush()
        self.fp.close()


class StringIteratorIO(io.TextIOBase):
    """ String iterated IO buffer """
    def __init__(self, iter):
        self._iter = iter
        self._buff = ''

    def readable(self):
        return True

    def _read1(self, n = None):
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n = None):
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)

        return ''.join(line)