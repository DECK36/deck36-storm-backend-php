#!/bin/sh

MYHOME=`echo ${PWD%/*/*/*}`
sed -e 's#/home/vagrant/.m2#'$MYHOME'/source/m2#g' < deck36-storm-backend-php.iml > deck36-storm-backend-php.iml.patch
mv deck36-storm-backend-php.iml.patch deck36-storm-backend-php.iml

