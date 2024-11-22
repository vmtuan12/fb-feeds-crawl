#!/bin/bash

yum -y update
yum -y install wget
rpm --import https://www.centos.org/keys/RPM-GPG-KEY-CentOS-Official
dnf install -y xorg-x11-server-Xvfb

mkdir -p /home/dev
mkdir /home/dev/venv
mkdir /home/dev/chrome_profiles

wget https://dl.google.com/linux/chrome/rpm/stable/x86_64/google-chrome-stable-131.0.6778.85-1.x86_64.rpm -O /home/dev/google-chrome.rpm
yum -y localinstall /home/dev/google-chrome.rpm

wget https://raw.githubusercontent.com/vmtuan12/venv/refs/heads/main/chromedriver_131_0_6778_69 -O /usr/bin/chromedriver

wget https://media.githubusercontent.com/media/vmtuan12/venv/refs/heads/main/fb_iso_env.tar.gz?download=true -O /home/dev/venv.tar.gz
tar xvzf /home/dev/venv.tar.gz -C /home/dev/venv
chmod 755 -R /home/dev/venv

rm -rf /home/dev/chromedriver*
rm -rf /home/dev/google-chrome.rpm
rm -rf /home/dev/venv.tar.gz