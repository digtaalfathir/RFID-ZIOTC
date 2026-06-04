#!/bin/bash
kill -2 `ps -ef 2> /dev/null | grep python3 | grep Httpkeyout.py | awk '{ print $2 }'`
