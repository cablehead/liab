import pytest

import liab


"""
i.user<id>
o.<id>room<id>

i.room<id>
o.<id>user<id>
o.<id>message<id>
"""

schema = {
    'user': {
        'typ': 'hash',
        'rooms': {
            'typ': 'bucket',
            'item': 'room',
        },
    },
    'room': {
        'typ': 'hash',
        'users': {
            'typ': 'bucket',
        },
        'messaegs': {
            'typ': 'stream',
        },
    },
}


def test_schema(tmp_path):
    print()
    store = liab.LIAB(str(tmp_path))
    with store.wx() as wx:
        s = liab.Session(schema, wx)
        pytest.raises(KeyError, lambda: s.foo)

        u1 = s.user.insert({'name': 'John'})
        pytest.raises(KeyError, lambda: u1.foo)
        assert u1.rooms.get() == []

        r1 = s.room.insert({'name': 'Group'})
        u1.rooms.set(r1)
        assert u1.rooms.get() == [r1._id]

        assert r1.users.get() == []
        r1.users.set(u1)
        assert r1.users.get() == [u1._id]


"""
def test_relation(tmp_path):
    s = liab.LIAB(str(tmp_path))
    with s.wx() as wx:
        r1 = wx.insert('room', {})
        r2 = wx.insert('room', {'name': 'Group'})
        u1 = wx.insert('user', {'name': 'John'})
        u2 = wx.insert('user', {'name': 'Tom'})
        u3 = wx.insert('person', {'name': 'Sam'})


def test_core(tmp_path):
    s = liab.LIAB(str(tmp_path))

    with s.wx() as wx:
        assert wx._id() < wx._id()
        want = [
            wx.insert('person', {'name': 'John'}),
            wx.insert('person', {'name': 'Tom'}),
            wx.insert('person', {'name': 'Sam'}),
            wx.insert('cat', {'name': 'Losh'}),
            wx.insert('cat', {'name': 'Hop'}), ]
        want.reverse()

        got = []
        for key in wx.get('cat'):
            got.append(key)
        for key in wx.get('person'):
            got.append(key)
        assert want == got

    with s.rx() as rx:
        got = []
        for key in rx.get('cat'):
            got.append(key)
        for key in rx.get('person'):
            got.append(key)
        assert want == got
"""
