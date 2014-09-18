#!/bin/sh

sudo mv ./lein /bin/lein

ln -s ../../deck36-php-web-app/app/config/ resources/config
ln -s ../../deck36-php-web-app/ resources/deck36-php-web-app

DIR=`pwd`

lein git-deps

cd $DIR/.lein-git-deps/storm-json	
mvn install

cd $DIR/.lein-git-deps/storm-rabbitmq
mvn install

lein deps
lein idea

cd $DIR

