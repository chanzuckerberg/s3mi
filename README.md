# S3MI

Transfer big files fast between S3 and EC2.

Pronounced *semi*.


# INSTALLATION

`pip install 'git+git://github.com/chanzuckerberg/s3mi.git'`


# COMMANDS


## `s3mi cp s3://huge_file destination`

  - fast 2GB/sec download from S3

  - may be constrained by destination write bandwidth

    * when the destination is an EBS gp2 volume,
      write bandwidth is only 250 MB/sec

    * RAID can increase that to 1.75GB/sec
      on select instance types [1]

    * suspected Linux kernel write limit 1.4 GB/sec [4]


## `s3mi cat s3://huge_file | some_command`

  - use cases

    * expand uncompressed or lz4-compressed archives

        `s3mi cat s3://gigabytes.tar | tar xf -`

    * stream through a really fast computation

        `s3mi cat s3://gigabytes_of_text | wc -l`

  - use lz4 instead of gzip or bzip2 in AWS;  faster methods like
    lz4 are better for the rich bandwidth/CPU ratio of EC2 and S3

## `s3mi raid array-name [number-of-slices] [slice-size]`

  Use RAID 0 over **EBS volumes** to overcome destination bandwidth limits.

  * Example:

      `s3mi raid my_raid 3 668`

    Creates 3 x 668 GB EBS gp2 volumes, RAIDs those together,
    and mounts on `/mnt/my_raid`.  The `my_raid` identifier
    must be unique across all your instances, and its
    slices will be named `my_raid_3_{0..2}`.

  * Lifecycle:

    Depending on the value of the instance `DeleteOnTermination` attribute
    the RAID volumes may be deleted when the instance terminates, or may
    persist and remain available to be mounted again under the special
    device `/dev/md127` on another instance.

  * Optimal RAID configuration:

    The ideal `number-of-slices` is the per-instance EBS bandwidth limit [1]
    divided by the per-volume EBS bandwidth limit [2].

    The `slice-size` must be large enough for the full per-volume
    bandwidth to remain available even after the volume's
    initial credits have been exhausted [2].

    In Dec 2018, the ideal settings for a c5.9xlarge with gp2 EBS are

        * number-of-slices = 3

        * slice-size >= 334 GB   (increase this if you need more IOPS or space)


## `s3mi raid array-name <block_device> <block_device> ...`

  Use RAID 0 with **instance NVME devices**.  For example,

  `s3mi raid my_raid /dev/nvme{1..8}n1`

  will create RAID 0 from the 8 slices `/dev/nvme{1..8}n1`,
  and mount it on `/mnt/my_raid`.

  In 2018, this is especially useful on i3.metal instances.

  To see what devices exist on the instance, try `lsblk`.

  Any pre-existing data on the devices will be lost.


## `s3mi tweak-vm`

  Configure VM parameters to delay the onset of synchronous (slow) I/O.
  This helps write operations complete faster through more aggressive
  caching.


# REFERENCES

  1. Per-instance EBS bandwidth limits
  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-ec2-config.html

  2. Per-volume EBS bandwidth limits
  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html

  3. Better Linux Disk Caching & Performance With vm.dirty_ratio & vm.dirty_background_ratio
  https://lonesysadmin.net/2013/12/22/better-linux-disk-caching-performance-vm-dirty_ratio/

  4. Toward Less Annoying Background Writeback
  https://lwn.net/Articles/682582/
