import pytest
from yaml import safe_load
from pathlib import Path
from syscalls import parse_cpu_set
from io_uring_assertions import (
    EXPECTED_IO_URING_SYSCALLS,
    assert_expected_io_uring_register_calls,
)

class SeastarCommandBuilder:
    async_workers_cpuset: str | None
    cpuset: str | None
    overprovisioned: bool

    def __init__(self):
        self.smp = None
        self.async_workers_cpuset = None
        self.cpuset = None
        self.overprovisioned = False

    def with_async_workers_cpuset(self, cpuset: str) -> "SeastarCommandBuilder":
        self.async_workers_cpuset = cpuset
        return self

    def with_cpuset(self, cpuset: str) -> "SeastarCommandBuilder":
        self.cpuset = cpuset
        return self

    def with_overprovisioned(self) -> "SeastarCommandBuilder":
        self.overprovisioned = True
        return self

    def build(self, seastar_path: Path) -> list[str]:
        args = [str(seastar_path)]
        if self.async_workers_cpuset is not None:
            args += ["--async-workers-cpuset", self.async_workers_cpuset]
        if self.cpuset is not None:
            args += ["--cpuset", self.cpuset]
        if self.overprovisioned:
            args += ["--overprovisioned"]
        args += ["--reactor-backend=asymmetric_io_uring"]
        return args


def assert_shard_cpu_distribution(
    allowed_cpuset: set[int],
    exclusive: bool,
    expected_count: int,
    mapping: list[dict[str, int]],
):
    available_cpus = allowed_cpuset.copy()
    for shard_info in mapping:
        shard_cpu = shard_info["cpu_id"]

        if shard_cpu in allowed_cpuset and shard_cpu not in available_cpus:
            raise AssertionError(
                f"CPU {shard_cpu} assigned more than once to shards in exclusive mode."
            )

        assert shard_cpu in allowed_cpuset, (
            f"Shard assigned to CPU {shard_cpu}, which is not in allowed cpuset {allowed_cpuset}."
        )

        if exclusive:
            available_cpus.remove(shard_cpu)

    assert len(mapping) == expected_count, (
        f"Expected {expected_count} shards, but found {len(mapping)}."
    )


def test_without_async_workers_cpuset_should_exit(run_process, seastar_path: Path):
    """
    Test: Verify that Seastar exits with error when --async-workers-cpuset is not provided.

    Context: The --async-workers-cpuset flag is mandatory for the asymmetric_io_uring backend.

    Expected behavior: Seastar should terminate with return code 1 and display an appropriate error message.
    """
    _, std_err, rc = run_process(
        SeastarCommandBuilder().build(seastar_path)
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert (
        "No CPUs specified for asymmetric_io_uring workers. Please see --async-workers-cpuset option."
        in std_err
    ), f"Unexpected seastar output:\n{std_err}"

@pytest.mark.parametrize(
    "cpuset,async_workers_cpuset",
    [
        # async_workers_cpuset specifies CPUs that are not in cpuset
        ("0", "1"),  # cpuset has CPU 0, async workers on CPU 1
        ("1", "0"),  # cpuset has CPU 1, async workers on CPU 0
        ("0,1", "2"),  # cpuset has CPUs 0-1, async workers on CPU 2,
        ("0,1", "1,2"),  # cpuset has CPUs 0-1, async workers on CPUs 1-2 (CPU 2 is not in cpuset)
    ],
)
def test_with_async_worker_cpuset_not_available_in_cpuset_should_exit_with_error(run_process, seastar_path: Path, cpuset: str, async_workers_cpuset: str):
    """
    Test: Verify that Seastar exits with error when --async-workers-cpuset specifies CPUs that are not available in the main cpuset.

    Context: If the CPUs specified for async workers are not part of the main cpuset, Seastar should not be able to initialize properly.
    """
    _, std_err, rc = run_process(
        SeastarCommandBuilder().with_cpuset(cpuset).with_async_workers_cpuset(async_workers_cpuset).build(seastar_path)
    )

    NOT_AVAILABLE_CPUS = set(parse_cpu_set(async_workers_cpuset)) - set(parse_cpu_set(cpuset))
    EXPECTED_ERROR_MESSAGE = (
        f"Bad value for --async-workers-cpuset: {NOT_AVAILABLE_CPUS.pop()} not allowed. Make sure it is a subset of the app's cpuset. Shutting down."
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert (
        EXPECTED_ERROR_MESSAGE
        in std_err
    ), f"Unexpected seastar output:\n{std_err}"

@pytest.mark.parametrize(
    "cpuset",
    [
        # All CPUs from taskset are assigned to async workers, leaving none for shards
        "0",  # Single CPU assigned to workers
        "1",  # Single CPU assigned to workers
        "0,1",  # Both CPUs assigned to workers
    ],
)
def test_with_the_same_cpuset_set_and_async_workers_cpuset_should_fail(
    run_process_with_strace,  # Fixture to run Seastar with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    cpuset: str,  # CPUs available through taskset (all assigned to async workers)
):
    """
    Test: Verify that Seastar fails when all available CPUs are assigned to async workers,
    leaving no CPUs for shards.

    Scenario: All CPUS allocated for async workers should be removed from cpuset for shards.
    If async_workers_cpuset equals taskset_cpuset, there are no CPUs available for
    running shard threads and Seastar should fail to initialize.

    Expected behavior: Seastar should fail to initialize with a 'bad cpuset' error.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_cpuset(cpuset)
        .with_async_workers_cpuset(cpuset)
        .build(seastar_path)
    )
    _, err, rc, _ = run_process_with_strace(
        seastar_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert "Could not initialize seastar: std::runtime_error (bad cpuset)" in err, (
        "Expected error message about no CPUs left for shards."
    )


@pytest.mark.parametrize(
    "cpuset,async_workers_cpuset,expected_shard_cpus,expected_workers_cpus",
    [
        # (cpuset: CPUs available for shards, async_workers_cpuset: CPUs for async workers,
        #  expected_shard_cpus: CPUs that shards will use, expected_workers_cpus: CPUs that workers will use)
        ("0,1", "0", {1}, "0"),  # Shard on CPU 1, worker on CPU 0
        ("0,1", "1", {0}, "1"),  # Shard on CPU 0, worker on CPU 1
    ],
)
def test_with_overlapping_cpuset_and_async_workers_cpuset_should_remove_overlapping_cpus(
    run_process_with_strace,  # Fixture to run process with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    cpuset: str,  # CPUs assigned to shards
    async_workers_cpuset: str,  # CPUs assigned to async uring workers
    expected_shard_cpus: set[int],  # Expected CPUs where shards will be pinned
    expected_workers_cpus: str,  # Expected CPUs where async workers will be pinned
):
    """
    Test: Verify that CPUs for async workers are removed from the shard CPU set.

    Scenario: When CPUs are assigned to both shards and async workers, the system should still operate
    but should warn about potential performance degradation due to CPU contention.

    Expected behavior: Seastar should run successfully, shards and workers should use their assigned CPUs,
    and a performance warning should appear in stderr.
    """
    std_out, std_err, rc, files = run_process_with_strace(
        SeastarCommandBuilder()
        .with_cpuset(cpuset)
        .with_async_workers_cpuset(async_workers_cpuset)
        .build(seastar_path),
        syscalls=EXPECTED_IO_URING_SYSCALLS,
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(std_out)
    assert_shard_cpu_distribution(expected_shard_cpus, True, len(mapping), mapping)

    expected_workers_cpus_set = parse_cpu_set(expected_workers_cpus)
    assert_expected_io_uring_register_calls(
        parse_syscalls,
        mapping,
        files,
        expected_workers_cpus_set,
        len(expected_workers_cpus_set),
    )
