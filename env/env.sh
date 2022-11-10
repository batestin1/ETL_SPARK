#!/bin/bash

###################################################################################################
#                                                                                                 #
# SCRIPT FILE: env.sh                                                                             #
# CREATION DATE: 08/11/2022                                                                       #
# HOUR: 09:25                                                                                     #
# DISTRIBUTION USED: UBUNTU                                                                       #
# OPERATIONAL SYSTEM: DEBIAN                                                                      #
#                                                                             DEVELOPED BY: BATES #
###################################################################################################
#                                                                                                 #
# SUMMARY: DATA ENGINEER ABOUT BR FEDERAL SERVERS GRATUITIES           #
#                                                                                                 #
###################################################################################################

# variables


COUNT=$(cat br_federal_servers_gratuities/env/packages.txt | sed ':a;N;s/\n/ /;ta;')
FILE=$(cat br_federal_servers_gratuities/env/packages.txt)

if  [ $? -eq 0 ] > /dev/null
then
    echo "Checkingn if the programs are installed properly"
    if which python3 > /dev/null
    then
        echo "Python Installed"
        python3 --version
        if which pip3 > /dev/null
        then
            echo "Pip Installed"
            pip3 --version
        fi

    else
        echo "Python Not Installed"
        sleep 2
        echo "Installing the software"
        sudo apt update -y
        sudo apt upgrade -y
        sudo apt install python3 -y
        sudo apt install python3-pip -y
        sudo apt install python3-venv -y
        clear
        sleep 2
        echo "Python Installed"
        python3 --version
    fi

    echo "Creating a environment"
    sleep 2
    python3 -m venv br_federal_servers_gratuities/python_env
    . br_federal_servers_gratuities/python_env/bin/activate



    echo "Environment call $NAME_ENV was created and activate with sucess!"
else
    echo "You get a error!"
fi


for i in $COUNT
do
    CHECK=$(pip show "$i" | grep -i "$i" | sed -n 1p | sed 's/Name://g')

    if [ $CHECK = "$i" ] 2>/dev/null
    then
        sleep 1
        echo "The package $i is already installed"
    else
        sleep 1
        echo "The package $i is not installed"
        sleep 1
        echo "Installing $i now"
        pip3 install $i
        sleep 1
        echo "The package $i is already installed"
    fi
done

