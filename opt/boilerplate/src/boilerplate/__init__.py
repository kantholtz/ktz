import logging
import logging.config
import os
import random
import warnings
from pathlib import Path

import pretty_errors
import yaml
from ktz.filesystem import path
from rich.console import Console

debug = False
version = "0.1.0"

_root_path = path(__file__).parent.parent.parent
assert _root_path.name == "boilerplate" and (_root_path / "src").is_dir()

#
# ENV VARS
#

ENV_DIR_DATA = "BOILERPLATE_DATA"

ENV_LOG_CONF = "BOILERPLATE_LOG_CONF"
ENV_LOG_FILE = "BOILERPLATE_LOG_FILE"


# check whether data directory is overwritten
if ENV_DIR_DATA in os.environ:
    _data_path = path(os.environ[ENV_DIR_DATA])
else:
    _data_path = path(_root_path / "data", create=True)


class _DIR:
    ROOT: Path = _root_path
    DATA: Path = path(_data_path, create=True)
    CONF: Path = path(_root_path / "conf", create=True)


class ENV:
    """Boilerplate environment."""

    DIR = _DIR


class BoilerplateError(Exception):
    """General error."""

    ...


#
#   --- CLI RELATED
#


# console is quiet by default
console = Console(quiet=True)


def tee(log_instance):
    def _tee(*messages: str, level=logging.INFO):
        for message in messages:
            log_instance.log(level, message)
        console.log(*messages, _stack_offset=2)

    return _tee


def setup_cli():
    import boilerplate

    boilerplate.init_logging()
    os.environ["PYTHONBREAKPOINT"] = "pudb.set_trace"

    pretty_errors.configure(
        filename_display=pretty_errors.FILENAME_EXTENDED,
        lines_after=2,
        line_number_first=True,
    )


#
#   --- LOG RELATED
#


log = logging.getLogger(__name__)


# if used as library do not log anything
log.addHandler(logging.NullHandler())


def init_logging():
    """Read the logging configuration from conf/ and initialize."""
    global log

    def _env(key, default):
        if key in os.environ:
            return os.environ[key]
        return default

    # expecting and removing the NullHandler
    assert len(log.handlers) == 1, "log misconfiguration"
    log.removeHandler(log.handlers[0])

    conf_file = _env(ENV_LOG_CONF, ENV.DIR.CONF / "logging.yaml")
    if not Path(conf_file).exists():
        return

    def _format_handler(conf, name: str):
        logfile = conf["handlers"][name]
        logfile["filename"] = _env(
            ENV_LOG_FILE,
            logfile["filename"].format(ENV=ENV),
        )

    with path(conf_file, is_file=True).open(mode="r") as fd:
        conf = yaml.safe_load(fd)

        _format_handler(conf, name="logfile-out")
        _format_handler(conf, name="logfile-err")

        logging.config.dictConfig(conf)
        logging.captureWarnings(True)

    log.info("logging initialized")  # logfile-out
    warnings.warn("logging initialized")  # logfile-err


#
# MISC
#
banner = """
 BOILERPLATE COMMAND LINE INTERFACE
"""


# rich console is quiet by default
console = Console(quiet=True)


# a "true" random number generator (if randomness is required after
# e.g. pytorch lightnings seed_everything or friends were called)
_randint = random.randint(int(1e5), int(1e6))
log.info(f"create global rng with seed >[{_randint}]<")
rng = random.Random()
