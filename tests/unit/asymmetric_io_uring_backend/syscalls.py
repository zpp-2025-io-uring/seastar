from pathlib import Path
from lark import Lark, Transformer


def parse_cpu_set(cpu_set: str) -> set[int]:
    cpus: set[int] = set()
    for part in cpu_set.split(","):
        if "-" in part:
            start, end = map(int, part.split("-"))
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(part))
    return cpus


class SyscallTransformer(Transformer):
    def TIMESTAMP(self, token):
        return float(token)

    def INT(self, token):
        return int(token)

    def HEX(self, token):
        # HEX token like 0x1c000
        return int(token, 16)

    def flag_enum(self, items):
        # items: sequence of FLAGNAME/HEX/CNAME tokens already transformed by their methods
        out = []
        for p in items:
            if isinstance(p, int):
                out.append(p)
            else:
                out.append(str(p))
        return out

    def struct_or_array(self, items):
        if len(items) == 1:
            return items[0]
        return items

    def FLAGNAME(self, token):
        return str(token)

    def CNAME(self, token):
        return str(token)

    def param(self, items):
        # items: [key, value]
        return (items[0], items[1])

    def param_value(self, items):
        if len(items) == 1:
            return items[0]
        return items

    def struct(self, items):
        if not items:
            return {}
        out = {}
        # items can contain tuples (key, value), lists of tuples, or dicts from nested structs
        for it in items:
            if isinstance(it, dict):
                out.update(it)
            elif isinstance(it, list):
                out.update(dict(it))
            elif isinstance(it, tuple):
                out[it[0]] = it[1]
        return out

    def array(self, items):
        if not items:
            return []
        # items can be a single list or multiple values
        if len(items) == 1 and isinstance(items[0], list):
            return items[0]
        return list(items)

    def io_uring_setup(self, items):
        # items: [fd, params, ret]
        entries, params, fd = items
        return {
            "call": "io_uring_setup",
            "fd": fd,
            "params": params,
            "entries": entries,
        }

    def io_uring_register(self, items):
        # items: [fd, op, param, count, ret]
        fd, op, param, count, ret = items
        return {
            "call": "io_uring_register",
            "fd": fd,
            "op": op,
            "param": param,
            "count": count,
            "ret": ret,
        }

    def syscall(self, items):
        if len(items) == 1:
            return items[0]
        return items

    def line(self, items):
        timestamp, call = items
        call["timestamp"] = timestamp
        return call


def parse_with_grammar(
    grammar_path: Path, lines: list[str]
) -> tuple[list[dict], list[dict]]:
    with open(grammar_path) as f:
        grammar = f.read()

    lark = Lark(grammar, start="line")
    transformer = SyscallTransformer()
    setup_calls: list[dict] = []
    register_calls: list[dict] = []
    for line in lines:
        tree = lark.parse(line)
        call = transformer.transform(tree)
        if call.get("call") == "io_uring_setup":
            setup_calls.append(call)
        elif call.get("call") == "io_uring_register":
            register_calls.append(call)
        else:
            raise ValueError(f"Unexpected syscall parsed: {call}")
    return setup_calls, register_calls


class io_uring_params:
    fd: int
    sq_thread: int | None
    wq_affinity: int | None  # We assume only one CPU in wq_affinity
    slaves: list[int] | None
    master_fd: int | None

    @classmethod
    def master(cls, fd: int, sq_thread_cpu: int, wq_affinity: int):
        io_uring = io_uring_params()
        io_uring.fd = fd
        io_uring.sq_thread = sq_thread_cpu
        io_uring.wq_affinity = wq_affinity
        io_uring.slaves = []
        io_uring.master_fd = None
        return io_uring

    @classmethod
    def slave(cls, fd: int, master_fd: int):
        io_uring = io_uring_params()
        io_uring.fd = fd
        io_uring.wq_affinity = None
        io_uring.sq_thread = None
        io_uring.master_fd = master_fd
        io_uring.slaves = None
        return io_uring

    def add_slave(self, slave_fd: int):
        if self.slaves is None:
            raise ValueError("Cannot add slave to non-master io_uring_params")
        self.slaves.append(slave_fd)

    def is_slave(self) -> bool:
        return self.master_fd is not None
