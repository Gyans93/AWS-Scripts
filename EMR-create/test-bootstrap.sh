#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
echo "installing packages"
sudo pip install -U \
  awscli            \
  boto              \
  ciso8601          \
  ujson             \
  workalendar

sudo yum install -y python-psycopg2
echo "installed packages"