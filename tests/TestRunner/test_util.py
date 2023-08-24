import logging
import random
import string
import time

alphanum_letters = string.ascii_letters + string.digits


def random_alphanum_string(length):
    return "".join(random.choice(alphanum_letters) for _ in range(length))


# attach a post-run trace checker to cluster that runs for events between the time of scope entry and exit
class ScopedTraceChecker:
    def __init__(self, cluster, checker_func, filename_substr: str = ""):
        self.cluster = cluster
        self.checker_func = checker_func
        self.filename_substr = filename_substr
        self.begin = None

    def __enter__(self):
        self.begin = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cluster.add_trace_check_from_to(
            self.checker_func, self.begin, time.time(), self.filename_substr
        )


_logger = None


def get_logger():
    global _logger
    if _logger is None:
        logging.basicConfig(
            level=logging.INFO,
            datefmt="%Y-%m-%dT%H:%M:%S",
            format="[%(levelname)s] %(asctime)s.%(msecs)03d %(message)s",
            handlers=[logging.StreamHandler()],
        )
        _logger = logging.getLogger("fdb.test")
    return _logger


def initialize_logger_level(logging_level):
    assert logging_level in ["DEBUG", "INFO", "WARNING", "ERROR"]
    logger = get_logger()
    if logging_level == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logging_level == "INFO":
        logger.setLevel(logging.INFO)
    elif logging_level == "WARNING":
        logger.setLevel(logging.WARNING)
    elif logging_level == "ERROR":
        logger.setLevel(logging.ERROR)


def version_tuple_from_str(ver_str):
    ver = [int(s) for s in ver_str.split(".")]
    assert len(ver) == 3, "Invalid version string {}".format(ver_str)
    return ver


def api_version_from_str(ver_str):
    # FIXME: simplify after everything is 8.0+
    ver_tuple = version_tuple_from_str(ver_str)
    if ver_tuple[0] >= 8 and ver_tuple[0] < 71:
        # handle 8.x -> 800000, as now the second version is the patch version
        ret = ver_tuple[0] * 100000
    elif ver_tuple[0] >= 71:
        # handle 71.3 -> 710300
        ret = ver_tuple[0] * 10000 + ver_tuple[1] * 100
    else:
        ret = ver_tuple[0] * 100 + ver_tuple[1] * 10
    return ret


def version_before(ver_str1, ver_str2):
    return version_tuple_from_str(ver_str1) < version_tuple_from_str(ver_str2)
