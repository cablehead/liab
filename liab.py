import logging
import ctypes
import time

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
        return Flake(int.from_bytes(key, byteorder='big'))

    def to_bytes(self):
        return super().to_bytes(7, byteorder='big')
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


def to_bytes(*a):
    ret = []
    for x in a:
        while hasattr(x, 'encode'):
            x = x.encode()
        if type(x) in [list, tuple]:
            ret.append(to_bytes(*x))
        else:
            ret.append(x)
    return b''.join(ret)


class DB:
    def __init__(self, tx, db):
        self.tx = tx
        self.db = db

    def cursor(self, *a, **kw):
        kw.setdefault('db', self.db)
        return self.tx.cursor(*a, **kw)

    def get(self, *a, **kw):
        kw.setdefault('db', self.db)
        return self.tx.get(*a, **kw)

    def put(self, *a, **kw):
        kw.setdefault('db', self.db)
        return self.tx.put(*a, **kw)


class Rx:
    def __init__(self, store):
        self.store = store
        self.spec = self.store.schema
        self.tx = self.store.env.begin()

    @property
    def o(self):
        return DB(self.tx, self.store.o)

    @property
    def i(self):
        return DB(self.tx, self.store.i)

    @property
    def m(self):
        return DB(self.tx, self.store.m)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.tx.abort()
        else:
            self.tx.commit()

    def __getattr__(self, name):
        d = self.spec[name]
        assert d['typ'] == 'hash'
        return Hash(self, d, [name])

    __getitem__ = __getattr__

    def __repr__(self):
        print('oh hai')
        return '<Session {}>'.format(self.path)


class Wx(Rx):
    def __init__(self, store):
        self.store = store
        self.spec = self.store.schema
        self.tx = self.store.env.begin(write=True)

    def _id(self):
        return next_id(
            0,
            lambda: self.m.get(b'flake'),
            lambda x: self.m.put(b'flake', x))



class Store:
    def __init__(self, schema, path):
        self.schema = schema
        self.env = lmdb.open(path, max_dbs=3)
        self.o = self.env.open_db(b'o')  # objects
        self.i = self.env.open_db(b'i')  # indices
        self.m = self.env.open_db(b'm')  # meta

    def rx(self):
        return Rx(self)

    def wx(self):
        return Wx(self)


class Stream:
    def __init__(self, session, spec, key):
        self.session = session
        self.spec = spec
        self.key = key

    def append(self, data):
        _id = self.session._id()
        item = Item(self.session, self.spec, self.key + [_id])
        item.set(data)
        return item

    def tail(self):
        prefix = to_bytes(self.key)
        c = self.session.i.cursor()
        c.set_range(prefix + Flake(256**7-1).to_bytes())
        if not c.key().startswith(prefix):
            c.prev()
        for key in c.iterprev(values=False):
            if not key.startswith(prefix):
                return
            yield Item(
                self.session,
                self.spec,
                self.key + [Flake.from_bytes(c.key()[len(prefix):])])


class Bucket:
    def __init__(self, session, spec, key):
        self.session = session
        self.spec = spec
        self.key = key

    def get(self):
        c = self.session.o.cursor()
        prefix = to_bytes(self.key)
        c.set_range(prefix)
        ret = []
        for key in c.iternext(values=False):
            if not key.startswith(prefix):
                break
            _id = Flake.from_bytes(key[len(prefix):])
            item = self.session[self.spec['item']][_id]
            ret.append(item)
        return ret

    def set(self, item):
        return self.session.o.put(to_bytes(self.key, item))


class Item:
    def __init__(self, session, spec, key):
        self.session = session
        self.spec = spec
        self.key = key
        self._id = key[-1]

    def __getattr__(self, name):
        d = self.spec[name]
        assert d['typ'] in ['bucket', 'stream']
        return {
            'bucket': Bucket,
            'stream': Stream,
        }[d['typ']](self.session, d, self.key + [name])

    def encode(self):
        return self._id

    def set(self, data):
        return self.session.i.put(to_bytes(self.key), msgpack.packb(data))

    def __eq__(self, other):
        return type(self) == type(other) and self._id == other._id


class Hash:
    def __init__(self, session, spec, key):
        self.session = session
        self.spec = spec
        self.key = key

    def __getitem__(self, _id):
        key = self.key + [_id]
        return Item(self.session, self.spec, key)

    def insert(self, data):
        _id = self.session._id()
        item = Item(self.session, self.spec, self.key + [_id])
        item.set(data)
        return item
