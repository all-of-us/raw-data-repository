#!/bin/bash

#
# Install and configure mysql
#
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get -y install lsb-release
mkdir ~/tmp_mysql && cd ~/tmp_mysql/
curl -LO https://dev.mysql.com/get/mysql-apt-config_0.8.14-1_all.deb
sudo debconf-set-selections <<< 'mysql-apt-config mysql-apt-config/select-preview select '
sudo debconf-set-selections <<< 'mysql-apt-config mysql-apt-config/select-product select Ok'
sudo debconf-set-selections <<< 'mysql-apt-config mysql-apt-config/select-server select mysql-5.7'
sudo debconf-set-selections <<< 'mysql-apt-config mysql-apt-config/select-tools select '
sudo debconf-set-selections <<< 'mysql-apt-config mysql-apt-config/unsupported-platform select abort'
dpkg -c mysql-apt-config_0.8.14-1_all.deb
sudo -E dpkg -i mysql-apt-config_0.8.14-1_all.deb
sudo apt-get update
sudo debconf-set-selections <<< "mysql-community-server mysql-community-server/root-pass password root"
sudo debconf-set-selections <<< "mysql-community-server mysql-community-server/re-root-pass password root"
sudo -E apt-get -y install mysql-community-server
sudo chmod 660 /var/log/mysql/error.log
sudo chmod 750 /var/log/mysql
sudo chmod 775 /var/log
sudo chmod 755 /var
export MYSQL_ROOT_PASSWORD=root
