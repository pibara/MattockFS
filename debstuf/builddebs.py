#!/bin/bash
#These two do exist in the pypy repo but not the Ubuntu repo
py2deb -r . fadvise
py2deb -r . pyblake2
py2deb -r . git+https://github.com/pibara/MattockFS
#Create a debian package for MattockFS itself (work in progress, untested)
mkdir -p mattockfs/DEBIAN
cp control postinst mattockfs/DEBIAN
chmod 555 mattockfs/DEBIAN/postinst
mkdir -p mattockfs/etc
cp ../mattockfs.json mattockfs/etc
mkdir -p mattockfs/usr/local/bin
cp ../bin/* mattockfs/usr/local/bin/
#mkdir -p mattockfs/usr/local/lib/python2.7/site-packages/mattock.egg-info
#cd ..
#python ./setup.py build
#env PYTHONPATH=debstuf/mattockfs/usr/local/lib/python2.7/site-packages:/usr/local/lib/python2.7/site-packages python ./setup.py install --prefix debstuf/mattockfs/usr/local
#cd debstuf
fakeroot dpkg-deb --build mattockfs mattockfs_`grep Version mattockfs/DEBIAN/control |sed -e 's/.* //'`.deb
rm -rf mattockfs/
#Download the primary dependencies of MattockFS (Work in progress, need to still look if complete for clean 16.04 install)
#apt-get download -o=dir::cache=. python-xattr python-redis python-fuse python-libewf fuse redis-server libewf2 libpython2.7 libbfio1 python-cffi python-cffi-backend python-pycparser python-ply libffi6 python-pkg-resources redis-tools libjemalloc1 libfuse2 python2.7 libpython2.7-stdlib libpython2.7-minimal python2.7-minimal python-minimal python libpython-stdlib
rm *.bin
tar czf ubuntu_packages_mattockfs_dfrwseu2017.tgz *.deb
rm *.deb

