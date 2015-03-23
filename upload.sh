#!/bin/bash

until $?; do

    php upload.php
    echo "Import halted.... restarting" >&2
    sleep 1
done