import liab


def test_core(tmp_path):
    print()
    print()
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
    print()
