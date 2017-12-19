# s3mi
Transfer big files fast between S3 and EC2.
Pronounced "semi".

# INSTALLATION

pip install 'git+git://github.com/chanzuckerberg/s3mi.git'

# COMMANDS


s3mi cp s3://huge_file destination

    -- fast 2GB/sec download from S3

    -- may be constrained by destination write bandwidth

        * when the destination is an EBS gp2 volume,
          write bandwidth is only 160 MB/sec

        * RAID can increase that to 1.75GB/sec
          on select instance types [1]


s3mi cat s3://huge_file | some_command

    -- use cases

        * expand uncompressed archives

            s3cat s3://gigabytes.tar | tar xf -

        * stream through a really fast computation

            s3cat s3://gigabytes_of_text | wc -l

    -- do not use for expanding compressed archives

        * typically gated by decompression or
          by destination, not by download

    -- use only when piping through another command

        * s3mi cp is better than s3mi cat
          for direct downloads

s3mi raid volume-name [N] [volume-size]

    Use RAID to overcome destination bandwidth limits.

    Example:

        s3mi raid my_fast_raid 7 214GB

    Creates 7 x 214GB EBS gp2 volumes, RAID0s those together,
    and mounts the set on /mnt/my_fast_raid.

    After the instance is restarted or terminated, the RAID array
    will persist, but will not be mounted.  To remount on either the
    original instance, or on another instance after the original
    instance has been terminated, just rerun the same command

        s3mi raid my_fast_raid 7 214GB

    Optimal configuration:

    The ideal N is the per-instance EBS bandwidth limit [1]
    divided by the per-volume EBS bandwidth limit [2].

    The ideal volume-size is chosen to ensure the per-volume
    bandwidth limit can be met in steady state [2].

    In Dec 2017, the ideal settings are as follows.

        c5.18xlarge with gp2 EBS

            N >= 7
            volume-size >= 214 GB

        i3.16xlarge with gp2 EBS

            N >= 11
            volume-size >= 214 GB


#REFERENCES

  1. Per-instance EBS bandwidth limits
	http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-ec2-config.html

	2. Per-volume EBS bandwidth limits
	http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
