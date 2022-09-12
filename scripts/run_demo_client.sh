#!/bin/bash
# Try to run the replicas as in run_demo.sh first and then run_demo_client.sh.
# Use Ctrl-C to terminate the proposing replica (e.g. replica 0). Leader
# rotation will be scheduled. Try to kill and run run_demo_client.sh again, new
# commands should still get through (be replicated) once the new leader becomes
# stable.


./examples/hotstuff-client --idx 0 --iter -1 --max-async 16

# ./examples/themis-client --idx 0 --iter -1 --max-async 4
# gdb -ex r -ex bt -ex q --args ./examples/themis-client --idx 0 --iter -1 --max-async 10
# valgrind --leak-check=full ./examples/themis-client --idx 0 --iter -1 --max-async 10
