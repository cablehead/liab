import liab


def test_core(tmp_path):
    print()
    print()
    s = liab.LIAB(str(tmp_path))

    with s.wx() as wx:
        assert wx._id() < wx._id()
        wx.insert('person', {'name': 'John'})
        wx.insert('person', {'name': 'Tom'})
        wx.insert('person', {'name': 'Sam'})

        wx.insert('cat', {'name': 'Losh'})
        wx.insert('cat', {'name': 'Hop'})

        for key in wx.get('cat'):
            print(key)

        print()

        for key in wx.get('person'):
            print(key)

    print()
