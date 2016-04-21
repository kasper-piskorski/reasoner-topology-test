#!/bin/bash
./kill.sh
mvn clean package -DskipTests
./clearLog.sh
./submit.sh
