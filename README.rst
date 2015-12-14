MattockFS Computer Forensics File-System

This code contains four main components:

* A core CarvPath library for working with Carvpath Annotations
* The core logic for the MattockFS file-system.
* The FUSE filesystem frontend exposing the core logic as filesystem.
* A python wrapper library for communicating with MattockFS and implementing
  computer forensic modules that run on top of MattockFS.

MattockFS is a Python rewrite of the old CarvFS CarvPath based user space 
filesystem that was buit to facilitate the Open Computer Forensics Architecture.
MattockFS combines the CarvPath based filesystem with a set of new features:

* Instead of MySql/Postgess for longpath storage, MattockFS uses Redis.
* MattockFS implements an OCFA-Anycast alike message bus as in-fs facility.
* MattockFS provides some base system and module load information to higher
  logical levles that should be used to throttle new data input and thus limit
  page-cache miss rates.
* MattockFS uses CarvPath property sortable sets rather than the priority queues
  used in the OCFA-Anycast. This should allow for a processing priority based
  on properties that match either the opportunistic hashing or other page-cache hit
  oriented scheduling strategies.
* MattockFS uses posix_fadvise in order to notify the kernel of archive chunks that 
  shall no longer be needed in the archive-bound page-cache.
* MattockFS implements opportunistic hashing. When low-level reads and writes are 
  performed, if possible open high level file objects will get hashed opportunistically.
* MattockFS implements a Sealed Digital Evidence Bag approach to both data and meta-data.
  Storage can be allocated once by the data creator and will need to be 'frozen' before
  being forwarded to other computer forensic modules.
* MattockFS uses a sparse capability based least authority programming interface. 
* MattockFS implements FS-level provenance logging. 




