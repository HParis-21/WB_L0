#!/bin/sh

orders=data/*.json

for files in $orders
do
    go run sending.go orders "$(cat $files)"
done
