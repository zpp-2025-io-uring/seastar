import pytest
from yaml import safe_load
from pathlib import Path
from syscalls import parse_cpu_set
from io_uring_assertions import (
    EXPECTED_IO_URING_SYSCALLS,
    assert_expected_io_uring_register_calls,
)

class SeastarCommandBuilder:
    smp: str | None
    async_workers_cpuset: str | None
    cpuset: str | None
    overprovisioned: bool

    def __init__(self):
        self.smp = None
        self.async_workers_cpuset = None
        self.cpuset = None
        self.overprovisioned = False

    def with_smp(self, smp: str) -> "SeastarCommandBuilder":
        self.smp = smp
        return self

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
        if self.smp is not None:
            args += ["--smp", self.smp]
        if self.async_workers_cpuset is not None:
            args += ["--async-workers-cpuset", self.async_workers_cpuset]
        if self.cpuset is not None:
            args += ["--cpuset", self.cpuset]
        if self.overprovisioned:
            args += ["--overprovisioned"]
        args += ["--reactor-backend=asymmetric_io_uring"]
        return args


def assert_shared_cpus_warning(cpuset: str, async_workers_cpuset: str, std_out: str):
    cpus_for_shards = parse_cpu_set(cpuset)
    cpus_for_async_workers = parse_cpu_set(async_workers_cpuset)
    shared_cpus = cpus_for_shards.intersection(cpus_for_async_workers)
    if not shared_cpus:
        raise AssertionError(
            "No shared CPUs found between cpuset and async_workers_cpuset. Test setup error."
        )

    expected_warning = f"The following CPUs assigned to shards overlap with the async workers cpuset: {','.join(str(cpu) for cpu in shared_cpus)}. This may lead to performance degradation. It is recommended to keep the main cpuset and async workers cpuset disjoint."
    assert expected_warning in std_out, (
        f"Expected warning about shared CPUs not found in output:\n{std_out}"
    )


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
        SeastarCommandBuilder().with_smp("2").build(seastar_path)
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert (
        "No CPUs specified for asymmetric_io_uring workers. Please see --async-workers-cpuset option."
        in std_err
    ), f"Unexpected seastar output:\n{std_err}"


def test_with_overprovisioned_without_smp_and_cpuset_should_fail(
    run_process, seastar_path: Path
):
    """
    Test: Verify that Seastar fails when overprovisioned mode is enabled without explicit --smp or --cpuset.

    Expected behavior: Seastar should exit with error code 1 and report that overprovisioned mode
    requires explicit CPU configuration when async workers are allocated.
    """
    _, std_err, rc = run_process(
        SeastarCommandBuilder()
        .with_overprovisioned()
        .with_async_workers_cpuset("0")
        .build(seastar_path)
    )

    assert rc == 1, f"Expected return code 1, got {rc}."
    assert (
        "Cannot run in overprovisioned mode when async workers are allocated and neither --smp nor --cpuset is specified"
        in std_err
    ), f"Unexpected seastar output:\n{std_err}"


@pytest.mark.parametrize(
    "taskset_cpuset",
    [
        # All CPUs from taskset are assigned to async workers, leaving none for shards
        "0",  # Single CPU assigned to workers
        "1",  # Single CPU assigned to workers
        "0,1",  # Both CPUs assigned to workers
    ],
)
def test_with_the_same_task_set_and_async_workers_cpuset_should_fail(
    run_process_with_strace,  # Fixture to run Seastar with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    taskset_cpuset: str,  # CPUs available through taskset (all assigned to async workers)
):
    """
    Test: Verify that Seastar fails when all available CPUs are assigned to async workers,
    leaving no CPUs for shards.

    Scenario: When async_workers_cpuset is provided without --smp or --cpuset, then
    all CPUS allocated for async workers should be removed from cpuset for shards.
    If async_workers_cpuset equals taskset_cpuset, there are no CPUs available for
    running shard threads and Seastar should fail to initialize.

    Expected behavior: Seastar should fail to initialize with a 'bad cpuset' error.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_async_workers_cpuset(taskset_cpuset)
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", taskset_cpuset] + seastar_args
    _, err, rc, _ = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
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
        ("0,1", "0", {0, 1}, "0"),  # Shard on CPU 0, worker on CPU 0
        ("0,1", "1", {0, 1}, "1"),  # Shard on CPU 1, worker on CPU 1
        ("0,1", "0,1", {0, 1}, "0,1"),  # Shards on CPUs 0-1, workers on CPUs 0-1
    ],
)
def test_with_overlapping_cpuset_and_async_workers_cpuset_should_work_fine_but_warn(
    run_process_with_strace,  # Fixture to run process with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    cpuset: str,  # CPUs assigned to shards
    async_workers_cpuset: str,  # CPUs assigned to async uring workers
    expected_shard_cpus: set[int],  # Expected CPUs where shards will be pinned
    expected_workers_cpus: str,  # Expected CPUs where async workers will be pinned
):
    """
    Test: Verify that overlapping cpuset and async_workers_cpuset work but generates a warning.

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
    assert_shared_cpus_warning(cpuset, async_workers_cpuset, std_err)

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


@pytest.mark.parametrize(
    "taskset_cpuset,smp",
    [
        # (taskset_cpuset: CPUs available via taskset, smp: number of shards to create)
        ("0", 1),  # Single CPU, single shard
        ("1", 1),  # Single CPU, single shard
        ("0,1", 2),  # Two CPUs, two shards
    ],
)
@pytest.mark.parametrize(
    "async_workers_cpuset",
    [
        # CPUs assigned to async uring workers (may overlap with taskset CPUs)
        "0",  # Worker on CPU 0
        "1",  # Worker on CPU 1
        "0,1",  # Workers on CPUs 0-1
    ],
)
def test_with_smp_and_async_workers_cpuset_should_not_remove_cpus_from_cpuset(
    run_process_with_strace,  # Fixture to run process with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    taskset_cpuset: str,  # CPUs available through taskset constraints
    smp: int,  # Number of shards to create
    async_workers_cpuset: str,  # CPUs assigned to async uring workers
):
    """
    Test: Verify that when --smp is specified, all requested shards use CPUs from taskset_cpuset.

    Scenario: When explicit shard count (--smp) is given, Seastar should allocate shards within
    the available CPU set, even if async_workers_cpuset overlaps with the taskset.

    Expected behavior: All shards should be placed on CPUs from the taskset, and async workers should
    use their assigned CPUs. The system should not remove CPUs from the available set based on
    async_workers_cpuset when --smp is explicitly provided.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_smp(str(smp))
        .with_async_workers_cpuset(async_workers_cpuset)
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", taskset_cpuset] + seastar_args
    std_out, _, rc, files = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(std_out)

    taskset_cpuset_set = parse_cpu_set(taskset_cpuset)
    assert_shard_cpu_distribution(taskset_cpuset_set, True, smp, mapping)

    expected_workers_cpus = parse_cpu_set(async_workers_cpuset)
    expected_workers_cpu_count = min(len(expected_workers_cpus), smp)
    assert_expected_io_uring_register_calls(
        parse_syscalls,
        mapping,
        files,
        expected_workers_cpus,
        expected_workers_cpu_count,
    )


@pytest.mark.parametrize(
    "taskset_cpuset,async_workers_cpuset",
    [
        # (taskset_cpuset: CPUs available via taskset, async_workers_cpuset: CPUs for async workers)
        ("0", "1"),  # Shards on CPU 0, workers on CPU 1
        ("1", "0"),  # Shards on CPU 1, workers on CPU 0
        ("0,1", "0"),  # Shards and workers on CPUs 0-1, but CPU 0 reserved for workers
        ("0,1", "1"),  # Shards and workers on CPUs 0-1, but CPU 1 reserved for workers
    ],
)
def test_with_only_async_workers_cpuset_should_remove_cpus_from_cpuset(
    run_process_with_strace,  # Fixture to run Seastar with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    taskset_cpuset: str,  # CPUs available through taskset constraints
    async_workers_cpuset: str,  # CPUs assigned to async uring workers (subset of taskset_cpuset)
):
    """
    Test: Verify that when only --async-workers-cpuset is specified (no --smp and no --cpuset), CPUs are removed
    from the shard CPU set to avoid contention.

    Scenario: When async_workers_cpuset is specified without explicit --smp and --cpuset, Seastar should
    automatically exclude those CPUs from the shard CPU set to reduce contention.

    Expected behavior: Shards should run on taskset_cpuset minus async_workers_cpuset. The number
    of shards should equal the number of remaining CPUs.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_async_workers_cpuset(async_workers_cpuset)
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", taskset_cpuset] + seastar_args
    std_out, _, rc, files = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(std_out)

    taskset_cpuset_set = parse_cpu_set(taskset_cpuset)
    allowed_workers_cpus = parse_cpu_set(async_workers_cpuset)

    expected_cpus_for_shards = taskset_cpuset_set - allowed_workers_cpus
    assert expected_cpus_for_shards, (
        "Test setup error: all CPUs in taskset_cpuset are expected to be used by async workers, leaving no CPU for shards."
    )

    assert_shard_cpu_distribution(
        expected_cpus_for_shards, True, len(expected_cpus_for_shards), mapping
    )
    assert_expected_io_uring_register_calls(
        parse_syscalls, mapping, files, allowed_workers_cpus, len(allowed_workers_cpus)
    )


@pytest.mark.parametrize(
    "taskset_cpuset,async_workers_cpuset,smp",
    [
        # All CPUs from taskset are assigned to async workers, leaving none for shards
        ("0", "0", 1),  # Single CPU assigned to workers
        ("1", "1", 1),  # Single CPU assigned to workers
        ("0,1", "0,1", 2),  # Both CPUs assigned to workers
    ],
)
def test_with_overprovisioned_and_smp_should_create_smp_count_shards(
    run_process_with_strace,  # Fixture to run Seastar with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    taskset_cpuset: str,  # CPUs available through taskset (all assigned to async workers)
    async_workers_cpuset: str,  # CPUs assigned to async uring workers
    smp: int,  # Number of shards to create

):
    """
    Test: Verify that Seastar with overprovisioned mode and --smp runs correctly.
    
    Scenario: When overprovisioned mode is enabled along with --smp and async_workers_cpuset,
    Seastar should create the specified number of shards despite on which CPUs the async workers are running.
    As in overprovisioned mode, thread_affinity is not strictly enforced, we can only expect the number of shards
    to match --smp, not the specific CPUs they run on.

    Expected behavior: Seastar should create the number of shards specified by --smp,
    regardless of the async_workers_cpuset configuration.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_async_workers_cpuset(async_workers_cpuset)
        .with_smp(str(smp))
        .with_overprovisioned()
        .build(seastar_path)
    )
    taskset_args = ["taskset", "-c", taskset_cpuset] + seastar_args
    out, _, rc, files = run_process_with_strace(
        taskset_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    mapping = safe_load(out)
    assert len(mapping) == smp, (
        f"Expected {smp} shards to be created in overprovisioned mode, but found {len(mapping)}."
    )

    expected_number_of_workers_cpus = min(len(parse_cpu_set(async_workers_cpuset)), smp)
    assert_expected_io_uring_register_calls(
        parse_syscalls,
        mapping,
        files,
        parse_cpu_set(async_workers_cpuset),
        expected_number_of_workers_cpus,
    )


@pytest.mark.parametrize(
    "async_workers_cpuset,cpuset",
    [
        # (async_workers_cpuset: CPUs for async workers, cpuset: CPUs for shards)
        ("0", "1"),  # Workers on CPU 0, shards on CPU 1
        ("1", "0"),  # Workers on CPU 1, shards on CPU 0
        ("0,1", "0,1"),  # Workers and shards on CPUs 0-1
    ],
)
def test_with_overprovisioned_and_cpuset_should_create_as_many_shards_as_cpuset_specifies(
    run_process_with_strace,  # Fixture to run Seastar with strace syscall tracing
    seastar_path: Path,  # Path to the Seastar test executable
    parse_syscalls,  # Fixture to parse syscalls from strace output
    async_workers_cpuset: str,  # CPUs assigned to async uring workers
    cpuset: str,  # Cpus assigned to shards

):
    """
    Test: Verify that Seastar with overprovisioned mode and --cpuset runs correctly.
    
    Scenario: When overprovisioned mode is enabled along with --cpuset and async_workers_cpuset,
    Seastar should create the specified number of shards despite on which CPUs the async workers are running.
    As in overprovisioned mode, thread_affinity is not strictly enforced, we can only expect the number of shards
    to match |--cpuset|, not the specific CPUs they run on.

    Expected behavior: Seastar should create the number of shards specified by |--cpuset|,
    regardless of the async_workers_cpuset configuration.
    """
    seastar_args = (
        SeastarCommandBuilder()
        .with_async_workers_cpuset(async_workers_cpuset)
        .with_cpuset(cpuset)
        .with_overprovisioned()
        .build(seastar_path)
    )
    out, _, rc, files = run_process_with_strace(
        seastar_args, syscalls=EXPECTED_IO_URING_SYSCALLS
    )

    assert rc == 0, f"Expected return code 0, got {rc}."

    expected_number_of_shards = len(parse_cpu_set(cpuset))
    mapping = safe_load(out)
    assert len(mapping) == expected_number_of_shards, (
        f"Expected {expected_number_of_shards} shards to be created in overprovisioned mode, but found {len(mapping)}."
    )

    expected_number_of_workers_cpus = min(len(parse_cpu_set(async_workers_cpuset)), expected_number_of_shards)
    assert_expected_io_uring_register_calls(
        parse_syscalls,
        mapping,
        files,
        parse_cpu_set(async_workers_cpuset),
        expected_number_of_workers_cpus,
    )
