import hashlib


def args_hash(*args) -> str:
    bytestr = "".join(map(str, args)).encode()
    return hashlib.sha224(bytestr).hexdigest()
