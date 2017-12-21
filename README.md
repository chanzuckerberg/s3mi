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
      write bandwidth is only 160 MB/sec

    * RAID can increase that to 1.75GB/sec
      on select instance types [1]


## `s3mi cat s3://huge_file | some_command`

  - use cases

    * expand uncompressed archives

        `s3mi cat s3://gigabytes.tar | tar xf -`

    * stream through a really fast computation

        `s3mi cat s3://gigabytes_of_text | wc -l`

  - do not use for expanding compressed archives

    * typically gated by decompression or
      by destination, not by download


## `s3mi raid volume-name [N] [volume-size]`

  Use RAID 0 to overcome destination bandwidth limits.

  * Example:

      `s3mi raid my_raid 7 214GB`

    Creates 7 x 214GB EBS gp2 volumes, RAIDs those together,
    and mounts the set on `/mnt/my_raid`.  The `my_raid` identifier
    must be unique across all your instances.

    After the instance is restarted or terminated, the RAID array
    will persist, but will not be mounted.  To remount on the
    original instance, or on another instance if the original
    instance has been terminated, just rerun the same command
    you used to create the array in the first place.
      
  * Optimal RAID configuration:

    The ideal `N` is the per-instance EBS bandwidth limit [1]
    divided by the per-volume EBS bandwidth limit [2].

    The `volume-size` must be large enough for the full per-volume
    bandwidth to remain available even after the volume's
    initial credits have been exhausted [2].

    In Dec 2017, the ideal settings are as follows.

      * c5.18xlarge with gp2 EBS

        * N >= 7
	
        * volume-size >= 214GB

      * i3.16xlarge with gp2 EBS

        * N >= 11
	
        * volume-size >= 214GB
	
  * Design question:  What if the instance where the RAID array
    is to be mounted shouldn't have permissions to create new EBS
    volumes?


# REFERENCES

  1. Per-instance EBS bandwidth limits
  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-ec2-config.html

  2. Per-volume EBS bandwidth limits
  http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
