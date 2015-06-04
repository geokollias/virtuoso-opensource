fdisk /dev/xvdb < np.cmd
fdisk /dev/xvdc < np.cmd
mkfs.ext4 /dev/xvdb1
mkfs.ext4 /dev/xvdc1
mount /dev/xvdb1 /1s1
mount /dev/xvdc1 /1s2
mkdir /1s1/dbs
mkdir /1s2/dbs
chown ec2-user /1s1/dbs
chown ec2-user /1s2/dbs
chown ec2-user /1s1
chown ec2-user /1s2
