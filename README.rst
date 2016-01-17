MattockFS Computer Forensics File-System

http://pibara.github.io/MattockFS/

If you want to install MattockFS on your (Ubuntu) system, run the script ubuntu_setup.
This script will do the following:

* Build and install a python module named mattock containing 'carvpath', 'api'  and all 
  the MattockFS files except for the starter and stopper.
* Create the user 'mattockfs' and its /var/mattock working directory.
* Patch /etc/fuse.conf if needed to contain user_allow_other directive.
* Install the non-pip dependencies fuse, fuse-dev and redis-server 
* Copy the start and stop scripts to /usr/local/bin

If you are on Ubuntu, running the script as follows should do the trick:

    ./ubuntu_setup

On any other Linux distro, follow the following steps:

  * install redis
  * install fuse and fuse libraries
  * Run 'python ./setup.py build'
  * Run 'sudo python ./setup.py install'
  * Create a new user named 'mattockfs' and make sure the user is allowed to run 
    fuse file-systems (/dev/fuse access rights)
  * Update /etc/fuse.conf as to 'allow_others'
  * Copy all the files in the bin directory to /ust/local/bin

If you wish to play with EWF files, you should also install the pyewf python module.

After successfully running this script or manually going through all the steps, 
you should be able to use start_mattockfs and stop_mattockfs respectively to start 
or stop MattockFS. You should be asked for your sudo sudo password if you call these.


To check if installation was fully sucessfull, you should first start MattockFS in the
background:

    start_mattockfs

And than run the base test script:

    test_mattockfs.py

You may also want to try adding a disk image to MattockFS and play with the CarvPath
section of MattockFS. Please note that ewf2mattock requires pyewf to be installed, a python
module that is NOT part of the standard installation dependency checks for MattockFS!

    ewf2mattock someimage.E01

It should return two relative paths for the meta and image data within MattockFS. 

