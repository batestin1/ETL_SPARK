#!/bin/bash

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: start.sh                                                                           #
# CREATION DATE: 08/11/2022                                                                       #
# HOUR: 09:25                                                                                     #
# DISTRIBUTION USED: UBUNTU                                                                       #
# OPERATIONAL SYSTEM: DEBIAN                                                                      #
#                                                                             DEVELOPED BY: BATES #
###################################################################################################
#                                                                                                 #
# SUMMARY: DATA ENGINEER ABOUT BR FEDERAL SERVERS GRATUITIES                                      #
#                                                                                                 #
###################################################################################################

# variables
DIR_NAME=$(ls | grep -i 'br')
DIR_FULL=$(pwd)/$DIR/
ARG=$(find $DIR_FULL -iname '*.json' | tail -n1)
DIR_ARG=$(basename $ARG)



if  [ $? -eq 0 ]; then > /dev/null
    sleep 1
    echo "Starting the Project"
    source $DIR_NAME/env/env.sh 2> $DIR_NAME/logs/history.log
    if [ -d $DIR_NAME/python_env ]; then 2> /dev/null
        sleep 1
        python3 $DIR_NAME/script/main.py $DIR_NAME $DIR_ARG 2>> $DIR_NAME/logs/history.log
        sleep 1
        echo "Finished Project"
    fi
fi

