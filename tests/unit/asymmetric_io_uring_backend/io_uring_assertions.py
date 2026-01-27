from collections.abc import Callable
from syscalls import io_uring_params
from pathlib import Path

EXPECTED_IO_URING_SYSCALLS = "io_uring_setup,io_uring_register"


def assert_expected_io_uring_register_calls(
    parse_syscalls: Callable[[list[str]], tuple[list[dict], list[dict]]],
    mapping,
    files,
    allowed_workers_cpus: set[int],
    expected_workers_cpus_count: int,
):
    """Assert that the expected io_uring_register calls are present in the strace output files.
    Shards are classified based on if they call io_uring_register with IORING_REGISTER_IOWQ_AFF:

    Master shard are expected to setup their io_uring with sq_thread=wq_affinity CPU
    and for that CPU to be in expected_workers_cpus set.

    Slave shards are expected to attach to the master's wq_fd.

    Additionally, all expected_workers_cpus should be used by master shards.
    Also, slave shards should be distributed evenly among master shards.

    :param parse_syscalls: Function to parse syscalls from strace output lines
    :type parse_syscalls: Callable[[list[str]], tuple[list[dict], list[dict]]]
    :param mapping: Shard mapping from seastar output
    :type mapping: list[dict]
    :param files: Mapping of thread_id to strace output file paths
    :type files: dict[int, Path]
    :param expected_workers_cpus: Set of expected async worker CPUs
    :type expected_workers_cpus: set[int]
    """

    number_of_master_shards = 0
    number_of_slave_shards = 0
    main_shard_found = False

    used_workers_cpus: set[int] = set()

    io_uring_params_per_fd: dict[int, io_uring_params] = {}
    for shard_info in mapping:
        thread_id = shard_info["thread_id"]
        if thread_id not in files:
            raise AssertionError(
                f"No strace output file found for shard with {shard_info}"
            )

        shard_id = shard_info["shard"]
        is_main_shard = shard_id == 0
        if is_main_shard:
            if main_shard_found:
                raise AssertionError(
                    "Multiple main shards (shard == 0) found in mapping."
                )
            main_shard_found = True

        setup_calls, register_calls = _get_syscalls(
            files[thread_id], is_main_shard, parse_syscalls
        )

        assert len(setup_calls) == 1, (
            f"Each shard should have exactly one io_uring_setup call, but shard_id={shard_id} has {len(setup_calls)}."
        )
        setup_call = setup_calls[0]

        is_master_shard = any(
            call["op"] == "IORING_REGISTER_IOWQ_AFF" for call in register_calls
        )
        if is_master_shard:
            number_of_master_shards += 1
            io_uring_param = _assert_master_io_uring_setup_call(
                setup_call, register_calls
            )
            assert io_uring_param.sq_thread in allowed_workers_cpus, (
                f"sq_thread CPU {io_uring_param.sq_thread} was not expected to be use as async worker."
            )
            assert io_uring_param.sq_thread not in used_workers_cpus, (
                f"sq_thread CPU {io_uring_param.sq_thread} is used by multiple master shards."
            )
            used_workers_cpus.add(int(io_uring_param.sq_thread))
        else:
            number_of_slave_shards += 1
            io_uring_param = _assert_slave_io_uring_setup_call(
                setup_call, register_calls
            )

        assert io_uring_param.fd not in io_uring_params_per_fd, (
            f"Duplicate io_uring fd {io_uring_param.fd} found for master shards."
        )
        io_uring_params_per_fd[io_uring_param.fd] = io_uring_param

    # Now, link slave shards to their master shards
    for io_uring in io_uring_params_per_fd.values():
        if not io_uring.is_slave():
            continue
        master_io_uring = io_uring_params_per_fd.get(io_uring.master_fd, None)
        assert master_io_uring is not None, (
            f"Slave shard with wq_fd={io_uring.master_fd} has no matching master shard."
        )

        master_io_uring.add_slave(io_uring.fd)

    assert main_shard_found, "No main shard (shard_id == 0) found in mapping."
    assert number_of_master_shards > 0, "No master shards found in mapping."
    assert len(used_workers_cpus) == expected_workers_cpus_count, (
        f"Expected {expected_workers_cpus_count} unique async worker CPUs to be used by master shards, but found {len(used_workers_cpus)}: {used_workers_cpus}."
    )

    _assert_even_distribution_of_slave_shards(
        io_uring_params_per_fd,
        expected_slaves_per_master=number_of_slave_shards // number_of_master_shards,
    )


def _get_syscalls(
    strace_output_file: Path,
    is_main_shard: bool,
    parse_syscalls: Callable[[list[str]], tuple[list[dict], list[dict]]],
) -> tuple[list[dict], list[dict]]:
    """Get io_uring_setup and io_uring_register syscalls from strace output file.

    :param strace_output_file: Path to strace output file
    :type strace_output_file: Path
    :param is_main_shard: Whether the shard is the main shard (shard_id == 0)
    :type is_main_shard: bool
    :param parse_syscalls: Function to parse syscalls from strace output lines
    :type parse_syscalls: Callable[[list[str]], tuple[list[dict], list[dict]]]
    :return: Tuple of (io_uring_setup_calls, io_uring_register_calls)
    :rtype: tuple[list[dict], list[dict]]
    """

    with open(strace_output_file, "r") as f:
        strace_output = f.read()

    lines = [
        line
        for line in strace_output.splitlines()
        if "io_uring_setup" in line or "io_uring_register" in line
    ]
    setup_calls, register_calls = parse_syscalls(lines)
    if is_main_shard:
        setup_calls, register_calls = _filter_out_syscalls_for_main_shard(
            setup_calls, register_calls
        )
    return setup_calls, register_calls


def _assert_master_io_uring_setup_call(
    setup_call: dict,
    register_calls: list[dict],
) -> io_uring_params:
    EXPECTED_FLAGS = [
        "IORING_SETUP_SQPOLL",
        "IORING_SETUP_SQ_AFF",
    ]
    sq_thread_cpu, wq_fd = _assert_io_uring_setup_call(setup_call, EXPECTED_FLAGS)
    assert wq_fd == -1, "Master shard io_uring_setup call should have wq_fd == -1."
    assert sq_thread_cpu >= 0, (
        "Master shard io_uring_setup call has invalid sq_thread_cpu."
    )

    register_wq_affinity_call = list(
        filter(lambda call: call["op"] == "IORING_REGISTER_IOWQ_AFF", register_calls)
    )
    if len(register_wq_affinity_call) != 1:
        raise AssertionError(
            "Master shard io_uring_setup call should have exactly one associated IORING_REGISTER_IOWQ_AFF call."
        )
    wq_affinity = register_wq_affinity_call[0]["param"]
    assert isinstance(wq_affinity, list), (
        "IORING_REGISTER_IOWQ_AFF call has invalid wq_affinity parameter."
    )
    assert len(wq_affinity) == 1, (
        "IORING_REGISTER_IOWQ_AFF call should have exactly one CPU in wq_affinity parameter."
    )

    assert wq_affinity[0] == sq_thread_cpu, (
        "IORING_REGISTER_IOWQ_AFF call wq_affinity does not match sq_thread_cpu from io_uring_setup."
    )
    return io_uring_params.master(setup_call["fd"], sq_thread_cpu, wq_affinity[0])


def _assert_slave_io_uring_setup_call(
    setup_call: dict,
    register_calls: list[dict],
) -> io_uring_params:
    EXPECTED_FLAGS = ["IORING_SETUP_SQPOLL", "IORING_SETUP_ATTACH_WQ"]
    sq_thread_cpu, wq_fd = _assert_io_uring_setup_call(setup_call, EXPECTED_FLAGS)
    assert sq_thread_cpu == 0, (
        "Slave shard io_uring_setup call should not set sq_thread_cpu explicitly."
    )

    assert wq_fd >= 0, "Slave shard io_uring_setup call has invalid wq_fd."

    register_wq_affinity_call = list(
        filter(lambda call: call["op"] == "IORING_REGISTER_IOWQ_AFF", register_calls)
    )
    assert len(register_wq_affinity_call) == 0, (
        "Slave shard io_uring_setup call should not have IORING_REGISTER_IOWQ_AFF call."
    )

    return io_uring_params.slave(setup_call["fd"], wq_fd)


def _assert_io_uring_setup_call(
    setup_call: dict, EXPECTED_FLAGS: list[str]
) -> tuple[int, int]:
    params = setup_call["params"]
    flags = params.get("flags", [])
    for flag in EXPECTED_FLAGS:
        assert flag in flags, f"io_uring_setup call missing expected flag: {flag}."

    sq_thread_cpu = params.get("sq_thread_cpu", -1)
    wq_fd = params.get("wq_fd", -1)
    return sq_thread_cpu, wq_fd


def _assert_even_distribution_of_slave_shards(
    io_uring_params_per_fd: dict[int, io_uring_params],
    expected_slaves_per_master: int,
):
    for io_uring_param in io_uring_params_per_fd.values():
        if io_uring_param.slaves is None:
            continue
        actual_slaves_count = len(io_uring_param.slaves)
        assert actual_slaves_count in [
            expected_slaves_per_master,
            expected_slaves_per_master - 1,
        ], (
            f"Master shard with fd={io_uring_param.fd} has {actual_slaves_count} slave shards, expected {expected_slaves_per_master}."
        )


def _filter_out_syscalls_for_main_shard(
    setup_calls: list[dict], register_calls: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Filter out syscalls belonging to the main shard (shard_id == 0).

    :param setup_calls: List of io_uring_setup calls
    :type setup_calls: list[dict]
    :param register_calls: List of io_uring_register calls
    :type register_calls: list[dict]
    :return: Tuple of filtered (setup_calls, register_calls)
    :rtype: tuple[list[dict], list[dict]]
    """

    # reactor_backend_selector::available() is called multiple times during startup
    AVAILABLE_CALLS_COUNT = 3
    SETUP_CALLS_PER_AVAILABLE_CALL = 2  # io_uring and asymmetric_io_uring
    IO_URING_REGISTER_CALLS_PER_AVAILABLE_CALLS_COUNT = (
        3  # probe for io_uring, probe and wq_affinity for asymmetric_io_uring
    )
    SETUP_CALLS_TO_IGNORE = SETUP_CALLS_PER_AVAILABLE_CALL * AVAILABLE_CALLS_COUNT
    REGISTER_CALLS_TO_IGNORE = (
        IO_URING_REGISTER_CALLS_PER_AVAILABLE_CALLS_COUNT * AVAILABLE_CALLS_COUNT
    )
    setup_calls = setup_calls[SETUP_CALLS_TO_IGNORE:]
    register_calls = register_calls[REGISTER_CALLS_TO_IGNORE:]
    return setup_calls, register_calls
