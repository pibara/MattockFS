MattockFS Computer Forensics File-System
========================================

MattockFS is a computer forensics actor-framework component, computer forensic data-repository and message-bus implemented as Fuse based user space file system. It is based partially on CarvFs and the AnyCast-relay from the Open Computer Forensics Architecture (OCFA). MattockFS uses CarvPath annotations to designate frozen repository data in the same way that CarvFS does. MattockFS was designed to address some of the shortcomings of OCFA in respect to disk-cache misses and access control, and as such aims to become an essential foundational component in future actor-model based computer forensic frameworks. MattockFS is not a complete computer forensics framework, rather MattockFS provides essential features that a computer forensics framework may build upon.

MattockFS provides the following facilities to future actor-model based computer forensic frameworks:

* **Lab-side privilege-separation equivalent of Sealed Digital Evidence Bags.** After creation, repository data is made immutable, thus guarding the integrity of the data from unintended write access by untrusted modules. 
* **Trusted provenance logs.** Actors/workers roles in the processing of digital evidence chunks are logged to a provenence log, leaving no opportunity for untrusted modules to falsify or corrupt provenance logs.
* **CarvPath based access to frozen (immutable) data.** Multi-layer CarvPath based access in the same way as provided by CarvFs.
* **Domain specific actors oriented localhost message bus.** MattockFS provides sparse-capability based access to an Anycast message bus aimed specifically at use by a computer forensics framework and the concept of toolchains. This is basically the same functionality that used to be provided by the Anycast-Relay in the Open Computer Forensics Architecture.
* **CarvPath based opportunistic hashing.** MattockFS maps all low-level reads and writes to reads and writes on all active (either open files or or part of an active tool-chain) CarvPaths and will opportunisticaly calculate BLAKE2 hashes for these CarvPaths when possible.
* **Page-cache friendly archive interaction.** MattockFS keeps track of CarvPaths belonging to tool-chains that are not yet completely done. It will communicate with the kernel when a toolchain completes and as a result part of the archive should be considered to be no longer active (and thus can be flushed from page cache).
* **Actor Job picking policies**: MattockFS implements multiple job CarvPath based picking policies aimed either at opportunistic hashing or page-cache load optimized strategies.
* **Load balancing support**: MattockFS allows a special actor, a load-balancer, to steal jobs from other (overloaded) actors in order to redistribute the job to an other node in a multi-host setup.
* **Throttle information**: MattockFS provides the overlaying computer forensic framework with meta-data concerning potential page-cache load and per Actor queue size and volume. Based on this information, actors should throttle their new-data output in order to avoid spurious page-cache misses caused by to much active evidence data at a time.
* **Hooks for a distributed FIVES router.** In the Open Computer Forensics Architecture a stateless router process was responsible for dynamic toolchain-path routing based on meta-data extracted from the evidence data. Later, the FIVES project created an alternative router process. This router carried state regarding the current location within a routing rule-list over to the next time the same data was processed by a router process. MattockFS provides a simple hook for use by an envisioned distributed version of FIVES-router like functionality.

MattockFS is not a complete forensic framework, it is a component that can be used as foundation for a complete forensic framework. Currently MattockFS is in beta. MattockFS comes with a Python API aimed at usage by an overlaying computer forensics framework. Future API's for other programming languages (C++ and others) are planned.

More detailed ingo can be found here:

http://pibara.github.io/MattockFS/

Install
=======

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

