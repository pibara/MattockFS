MattockFS Computer Forensics File-System
========================================


In order to run pymattockfs, you should first do the following.


* Make sure fuse and python fuse are installed. Uncomment 'user\_allow\_other' in the /etc/fuse.conf is active.

* Make sure the Redis server is installed and running on localhost.

* Make sure the following python modules are installed on the system:

  + python-xattr (note: NOT python-pyxattr)
  + python-redis

* To install the python blake2 module, first install python-pip. You can than install pyblake2 using pip.

* Create a user named 'mattockfs'. If your system uses the group 'fuse' than mattockfs must become a member.
  This is the case on Ubuntu 14.04. Also this user should end up owning /var/mattock. On Ubuntu 14.04 use the
  command :

  sudo useradd mattockfs -g fuse -d /var/mattock -m

  Alternatively, if no 'fuse' group is used on your system, for example in Ububtu 15.10, ommit the -g flag:

  sudo useradd mattockfs -d /var/mattock -m

After that, you may start the file-system and make it run as the new user:

  sudo su mattockfs -c './pymattockfs.py -o allow\_other'

Or if you suspect there to be bugs (MattockFS is currently still in alpha testing):
 
  sudo su mattockfs -c './pymattockfs.py -o allow\_other -f'

The file-system will be mounted under /var/mattock/mnt/0 and can be unmounted using:

  sudo su mattockfs -c 'fusermount -u /var/mattock/mnt/0'


