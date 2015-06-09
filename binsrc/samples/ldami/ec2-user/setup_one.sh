#!/bin/bash

echo '127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4' > /etc/hosts
echo '::1         localhost localhost.localdomain localhost6 localhost6.localdomain6' >> /etc/hosts
echo >> /etc/hosts

hostname=$(hostname)
echo "127.0.0.1   $hostname" >> /etc/hosts
tail -n +2 /home/ec2-user/hosts.conf | awk '{ print $0, "a" FNR}' >> /etc/hosts


