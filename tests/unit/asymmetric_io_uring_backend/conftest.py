import subprocess
import pytest
from typing import Callable, Tuple
from pathlib import Path
from syscalls import parse_with_grammar


def pytest_addoption(parser):
    parser.addoption(
        "--strace-path",
        required=True,
        help="Path to the strace executable",
    )
    parser.addoption(
        "--seastar-path",
        required=True,
        help="Path to the executable of some Seastar program",
    )
    parser.addoption(
        "--grammar-path",
        required=True,
        help="Path to the Lark grammar file",
    )


@pytest.fixture
def strace_path(request) -> Path:
    return Path(request.config.getoption("--strace-path"))


@pytest.fixture
def seastar_path(request) -> Path:
    return Path(request.config.getoption("--seastar-path"))


@pytest.fixture
def grammar_path(request) -> Path:
    return Path(request.config.getoption("--grammar-path"))


def _run(args: list[str], timeout: float = 30.0) -> Tuple[str, str, int]:
    proc = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise
    print(f"Command {' '.join(args)} exited with code {proc.returncode}")
    print(f"stdout:\n{stdout}")
    print(f"stderr:\n{stderr}")
    return stdout, stderr, proc.returncode


@pytest.fixture
def run_process() -> Callable[[list[str], float], Tuple[str, str, int]]:
    """Return a helper that runs a command and returns (stdout, stderr, returncode).

    Usage:
        stdout, stderr, rc = run_process(args, timeout=30.0)
    """

    return _run


@pytest.fixture
def run_process_with_strace(
    strace_path: Path, tmp_path: Path
) -> Callable[[list[str], str, float], Tuple[str, str, int, dict[int, Path]]]:
    """Return a helper that runs a command with strace and returns (stdout, stderr, returncode, files).

    Usage:
        stdout, stderr, rc, files = run_process_with_strace(args, syscalls="io_uring_register", timeout=30.0)
    """

    def _run_with_strace(
        args: list[str], syscalls: str, timeout: float = 30.0
    ) -> Tuple[str, str, int, dict[int, Path]]:
        # Create if does not exist
        if tmp_path.is_dir() is False:
            tmp_path.mkdir(parents=True, exist_ok=True)

        STRACE_OUTPUT_FILENAME = "strace_output.txt"

        strace_args = [
            str(strace_path),
            "-ff",
            "-s",
            "4096",
            "-ttt",
            "-e",
            syscalls,
            "-o",
            str(tmp_path / STRACE_OUTPUT_FILENAME),
        ]
        full_args = strace_args + args
        stdout, stderr, rc = _run(full_args, timeout=timeout)

        files = list(tmp_path.glob("strace_output.txt*"))
        mapped_files: dict[int, Path] = {}
        for file in files:
            pid_part = file.name.replace(STRACE_OUTPUT_FILENAME, "")
            if not pid_part.startswith("."):
                raise RuntimeError(f"Unexpected strace output file name: {file.name}")
            pid_str = pid_part[1:]  # remove leading dot
            if not pid_str.isdigit():
                raise RuntimeError(f"Unexpected strace output file name: {file.name}")
            pid = int(pid_str)
            mapped_files[pid] = file
        return stdout, stderr, rc, mapped_files

    return _run_with_strace


@pytest.fixture
def parse_syscalls(
    grammar_path: Path,
) -> Callable[[list[str]], tuple[list[dict], list[dict]]]:
    """Return a function that parses io_uring_setup and io_uring_register syscalls from strace output lines.
    :param grammar_path: Path to the Lark grammar file
    :type grammar_path: Path
    :return: Function that takes list of strace lines and returns tuple of two lists: (io_uring_setup_calls, io_uring_register_calls)
    :rtype: Callable[[list[str]], tuple[list[dict], list[dict]]]
    """

    def _parse(lines: list[str]) -> tuple[list[dict], list[dict]]:
        return parse_with_grammar(grammar_path, lines)

    return _parse
