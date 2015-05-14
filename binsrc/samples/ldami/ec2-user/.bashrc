# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions
export JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.79-2.5.5.1.el7_1.x86_64/jre"
export EC2_HOME=/usr/local/ec2/ec2-api-tools-1.7.3.2
export PATH=$PATH:$EC2_HOME/bin
