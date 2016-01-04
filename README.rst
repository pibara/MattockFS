MattockFS Computer Forensics File-System

http://pibara.github.io/MattockFS/

WARNING: The code-base should currently be considered late alpha. A first public beta is expected to be released soon.

If you want to install MattockFS on your (Ubuntu) system, run the script ubuntu_setup.
This script will do the following:

* Build and install a python module named mattock containing 'carvpath', 'api'  and all 
  the MattockFS files except for the starter and stopper.
* Create the user 'mattockfs' and its /var/mattock working directory.
* Patch /etc/fuse.conf if needed to contain user_allow_other directive.
* Install the non-pip dependencies fuse python-fuse and redis-server 
* Copy the start and stop scripts to /usr/local/bin


After successfully running this script, you should be able to use start_mattockfs  
and stop_mattockfs respectively to start or stop MattockFS.

