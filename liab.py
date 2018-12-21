import logging
import ctypes
import time
import sys

import msgpack
import lmdb


log = logging.getLogger(__name__)


class Fity3(ctypes.Structure):
    _fields_ = [
        ('sequence', ctypes.c_uint),
        ('last', ctypes.c_ulonglong), ]


class Fity3Exception(Exception):
    pass


# Wed, 15 Oct 2014 11:00:00.000 GMT
fitepoch = 1413370800000

worker_id_bits = 8
max_worker_id = -1 ^ (-1 << worker_id_bits)
sequence_bits = 4
worker_id_shift = sequence_bits
timestamp_left_shift = sequence_bits + worker_id_bits
sequence_mask = -1 ^ (-1 << sequence_bits)
timestamp_mask = -1 >> timestamp_left_shift << timestamp_left_shift


class Flake(int):
    @staticmethod
    def from_bytes(key):
        return Flake(int.from_bytes(key, 'big'))

    def to_bytes(self):
        return super().to_bytes(7, 'big')
    encode = to_bytes

    def to_timestamp(self):
        ret = self >> timestamp_left_shift
        ret += fitepoch
        ret = ret / 1000
        return ret


def next_id(
        worker_id,
        get,
        put,
        sleep=lambda x: time.sleep(x/1000.0),
        now=lambda: int(time.time()*1000)):

    assert worker_id >= 0 and worker_id <= max_worker_id

    try:
        state = get()
        if not state:
            state = Fity3()
        else:
            state = Fity3.from_buffer_copy(state)

        timestamp = now()

        if state.last > timestamp:
            raise Fity3Exception(
                'clock is moving backwards. waiting until {}'.format(
                    state.last),
                state.last - timestamp)

        if state.last == timestamp:
            state.sequence = (state.sequence + 1) & sequence_mask
            if state.sequence == 0:
                raise Fity3Exception('sequence overrun', 1)
        else:
            state.sequence = 0

        state.last = timestamp

        put(state)

        return Flake(
            ((timestamp-fitepoch) << timestamp_left_shift) |
            (worker_id << worker_id_shift) |
            state.sequence)

    except Fity3Exception as e:
        message, wait = e.args
        log.warning(message)
        sleep(wait)
        return next_id(worker_id, get, put, sleep=sleep, now=now)


class LMDB:
    class Env:
        def __init__(self, env):
            self.env = env

        def rx(self):
            return LMDB.Tx(self.env)

        def open_db(self, *a, **kw):
            return self.env.open_db(*a, **kw)

    class Tx:
        def __init__(self, env, write=False):
            self.env = env
            self.write = write

        def cursor(self):
            return self.tx.cursor()

        def get(self, key, *a, **kw):
            return self.tx.get(LMDB.to_bytes(key), *a, **kw)

        def put(self, key, *a, **kw):
            return self.tx.put(LMDB.to_bytes(key), *a, **kw)

        def delete(self, *a, **kw):
            return self.tx.delete(*a, **kw)


def to_bytes(*key):
    return b''.join(getattr(x, 'encode', lambda: x)() for x in key)


class LIAB:
    def __init__(self, path):
        self.env = lmdb.open(path, max_dbs=3)
        self.o = self.env.open_db(b'o')
        self.i = self.env.open_db(b'i')
        self.m = self.env.open_db(b'm')

    def rx(self):
        return LIAB.Rx(self)

    def wx(self):
        return LIAB.Wx(self)

    class Rx:
        def __init__(self, store):
            self.store = store
            self.tx = self.store.env.begin()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            if exc_type:
                self.tx.abort()
            else:
                self.tx.commit()

        def get(self, name):
            prefix = to_bytes(name)
            c = self.tx.cursor(db=self.store.o)

            c.set_range(prefix + Flake(256**7-1).to_bytes())
            if not c.key().startswith(prefix):
                c.prev()

            for key in c.iterprev(values=False):
                if not key.startswith(prefix):
                    return
                yield Flake.from_bytes(c.key()[len(prefix):])

    class Wx(Rx):
        def __init__(self, store):
            self.store = store
            self.tx = self.store.env.begin(write=True)

        def _id(self):
            return next_id(
                0,
                lambda: self.tx.get(b'flake', db=self.store.m),
                lambda x: self.tx.put(b'flake', x, db=self.store.m))

        def insert(self, name, data):
            _id = self._id()
            self.tx.put(
                to_bytes(name, _id),
                msgpack.packb(data),
                db=self.store.o)
            return _id
