#!/bin/bash
perl -e 'print "\0REQ\0\0\0\001\0\0\0\027do_get_test_resource_v1"' | nc localhost 8888