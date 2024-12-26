import logging
import os
import re
import sys
from functools import partial
from pathlib import Path

import pretty_errors
import pudb
import rich_click as click
from ktz.filesystem import path

import boilerplate

boilerplate.setup_cli()

log = logging.getLogger(__name__)
tee = boilerplate.tee(log)

os.environ["PYTHONBREAKPOINT"] = "pudb.set_trace"

pretty_errors.configure(
    filename_display=pretty_errors.FILENAME_EXTENDED,
    lines_after=2,
    line_number_first=True,
)


# ---


@click.group()
@click.option(
    "-d",
    "--debug",
    is_flag=True,
    default=False,
    help="activate debug mode (drop into pudb on error)",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    default=False,
    help="suppress console output",
)
def main(debug: bool, quiet: bool):
    """Boilerplate command line interface."""
    boilerplate.debug = debug
    boilerplate.console.quiet = quiet

    boilerplate.console.print(boilerplate.banner)
    boilerplate.console.log(f"initialized root path: {boilerplate.ENV.DIR.ROOT}")
    boilerplate.console.log(f"executing from: {os.getcwd()}")


# KTZ_BOILERPLATE_DELETE >>>
_DEL_MARKER_START = "KTZ_BOILERPLATE_DELETE >>>"
_DEL_MARKER_END = "KTZ_BOILERPLATE_DELETE <<<"


@main.command("replicate")
@click.argument("name")
@click.argument("destination")
def main_replicate(name, destination):
    """Create copy of this boilerplate with a different name."""
    new_root = path(destination, exists=False, create=True)
    old_root = boilerplate.ENV.DIR.ROOT

    def _read_replace_write(
        old_fpath: Path,
        new_fpath: Path,
    ):
        with old_fpath.open(mode="r") as fd:
            s = fd.read()

        # replace boilerplate occurences with new name
        for fn in (str.capitalize, str.upper, str.lower):
            s = s.replace(fn("boilerplate"), fn(name))

        # remove marked sections
        lines, buf = s.split("\n")[::-1], []
        while lines:
            line = lines.pop()
            if _DEL_MARKER_START in line:
                while _DEL_MARKER_END not in line:
                    lines.pop()
                continue

            buf.append(line)

        with new_fpath.open(mode="w") as fd:
            fd.write("\n".join(buf))

    exclude = {"/data", "README.md", "__pycache__"}
    rename = {"README.template.md": "README.md"}

    cprint = partial(boilerplate.console.print, end="")
    for old_path in old_root.glob("**/*"):
        old_relpath = old_path.relative_to(old_root)
        cprint(str(old_relpath))

        # Create the target path
        new_path = new_root / old_relpath
        if any(ex in "/" + str(old_relpath) for ex in exclude):
            cprint(" skipping\n", style="red")
            continue

        parts = list(new_path.parts)
        if "boilerplate" in new_path.parts:
            cprint(" moved folder", style="blue")
            parts[parts.index("boilerplate")] = name.lower()
            new_path = Path(*parts)

        if new_path.name in rename:
            cprint(f" renaming to {rename[new_path.name]}", style="green")
            new_path = new_path.parent / rename[new_path.name]

        if old_path.is_dir():
            cprint(" created", style="green")
            new_path.mkdir(exist_ok=True)

        else:
            cprint(" adjusted and copied", style="green")
            _read_replace_write(old_fpath=old_path, new_fpath=new_path)

        cprint("\n")


# KTZ_BOILERPLATE_DELETE <<<


def entry():
    try:
        main()  # pyright: ignore

    except Exception as exc:
        if not boilerplate.debug:
            raise

        log.error("debug: catched exception, starting debugger")
        log.error(str(exc))

        _, _, tb = sys.exc_info()
        pudb.post_mortem(tb)

        raise exc

    tee("exiting gracefully")
