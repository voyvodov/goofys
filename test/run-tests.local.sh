#!/bin/bash

#set -o xtrace
set -o errexit
set -o nounset

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'
cleanup () {
  docker-compose -p goofys-ci kill
  docker-compose -p goofys-ci rm -f
}

trap 'cleanup ; printf "${RED}Tests Failed For Unexpected Reasons${NC}\n"' HUP INT QUIT PIPE TERM

docker-compose -p goofys-ci -f test/docker-compose.tests.yml build 

docker-compose -p goofys-ci -f test/docker-compose.tests.yml up -d
if [ $? -ne 0 ] ; then
  printf "${RED}Docker Compose Failed${NC}\n"
  exit -1
fi
printf "${GREEN}Running tests...${NC}\n"
docker logs -tf goofys-ci-tests-1

TEST_EXIT_CODE=`docker wait goofys-ci-tests-1`
docker logs goofys-ci-tests-1
if [ -z ${TEST_EXIT_CODE+x} ] || [ "$TEST_EXIT_CODE" -ne 0 ] ; then
    printf "${RED}Tests Failed${NC} - Exit Code: $TEST_EXIT_CODE\n"
else
    printf "${GREEN}Tests Passed${NC}\n"
fi
cleanup
exit $TEST_EXIT_CODE
