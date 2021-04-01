#!/bin/bash
for i in {1..50}
do
    go test -run TestUnreliableChurn2C
done
