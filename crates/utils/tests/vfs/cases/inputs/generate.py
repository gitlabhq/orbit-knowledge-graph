import gzip
from pathlib import Path


ROOT = Path(__file__).parent


def entry(path, content=b"", kind=b"0", target="", size=None):
    header = bytearray(512)
    header[:len(path)] = path.encode()
    header[100:108] = b"0000644\0"
    header[108:116] = b"0000000\0"
    header[116:124] = b"0000000\0"
    header[124:136] = f"{len(content) if size is None else size:011o}\0".encode()
    header[136:148] = b"00000000000\0"
    header[148:156] = b"        "
    header[156:157] = kind
    header[157:157 + len(target)] = target.encode()
    header[257:265] = b"ustar\0" + b"00"
    header[148:156] = f"{sum(header):06o}\0 ".encode()
    return bytes(header) + content + b"\0" * (-len(content) % 512)


def archive(name, *entries):
    data = b"".join(entries) + bytes(1024)
    (ROOT / f"{name}.tar.gz").write_bytes(gzip.compress(data, mtime=0))


archive("absolute", entry("/root/escape", b"x"))
archive("traversal", entry("root/../../escape", b"x"))
archive("root-traversal", entry("../escape", b"x"))
archive("hardlink-escape", entry("root/link", kind=b"1", target="../secret"))
archive(
    "links",
    entry("root/src/file", b"content"),
    entry("root/alias", kind=b"1", target="root/src/file"),
    entry("root/chain", kind=b"2", target="alias"),
    entry("other/file", b"ignored"),
)
archive(
    "pax-headers",
    entry("root/pax_global_header", b"comment=x\n", kind=b"g"),
    entry("root/PaxHeader/file", b"path=root/file\n", kind=b"x"),
    entry("root/file", b"content"),
)
archive(
    "pax-size",
    entry("root/PaxHeader", b"13 size=4096\n", kind=b"x"),
    entry("root/big.txt", b"a" * 4096, size=0),
)
body = b"abcdefgh" * 1500 + b"end of complete body\n"
(ROOT / "complete-body.txt").write_bytes(body)
archive("complete-body", entry("root/big.txt", body), entry("root/logo.png", b"image"))
(ROOT / "truncated.tar.gz").write_bytes(b"\x1f\x8b\x08")
(ROOT / "empty.tar.gz").write_bytes(b"")
