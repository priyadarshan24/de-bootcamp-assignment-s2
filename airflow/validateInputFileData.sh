#!/bin/bash
OUTPUT=$(jot -r 1 0 1)

if [ $OUTPUT == 0 ]
then
    echo 0
else
    echo 99
fi
