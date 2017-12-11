#!/bin/bash

ps aux | grep -e "pidstat" | grep -v "grep" | grep -v "$$" | awk '{print $2}' | while read line ; do kill -2 $line > /dev/null; done
ps aux | grep -e "sar" | grep -v "grep" | grep -v "$$" | awk '{print $2}' | while read line ; do kill -2 $line > /dev/null; done

