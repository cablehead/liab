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
            'item': 'user',
        },
        'messages': {
            'typ': 'stream',
        },
    },
}


def test_core(tmp_path):
    store = liab.Store(schema, str(tmp_path))
    with store.wx() as wx:
        pytest.raises(KeyError, lambda: wx.foo)

        u1 = wx.user.insert({'name': 'John'})
        pytest.raises(KeyError, lambda: wx.user[u1].foo)
        assert wx.user[u1].rooms.get() == []

        r1 = wx.room.insert({'name': 'Group'})
        wx.user[u1].rooms.set(r1)
        assert wx.user[u1].rooms.get() == [r1]

        assert wx.room[r1].users.get() == []
        wx.room[r1].users.set(u1)
        assert wx.room[r1].users.get() == [u1]

        m1 = wx.room[r1].messages.append({'body': 'message 1'})
        m2 = wx.room[r1].messages.append({'body': 'message 2'})
        m3 = wx.room[r1].messages.append({'body': 'message 3'})
        m4 = wx.room[r1].messages.append({'body': 'message 4'})
        assert list(wx.room[r1].messages.tail()) == [m4, m3, m2, m1]
