setup_file() {
    go build
}

setup() {
    # TODO: Use this instead: https://github.com/ztombol/bats-docs#homebrew
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'
    load test_helper
}

teardown() {
    stop_cmds
}

@test "place" {
    start_node 8001

    # Try to write something to the node. This should fail, because no ranges
    # are assigned.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Run a single rebalance cycle.
    ./kv -controller -addr ":9000" -once

    # Try the same write again. It should succeed this time, because the
    # controller has assigned the first (infinite) range to it.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
}

@test "move" {
    start_node 8001
    start_node 8002
    start_controller 9000

    # Write a key.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success

    # Check that we can't write it to node 2.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Move the range from node 1 to node 2.
    run bin/client.sh 9000 ranger.Balancer.Move '{"range": {"key": 1}, "node": "8002"}'

    # TODO: Make the RPC handler (optionally?) block until the op is done, so we
    # don't have to do dumb things like this.
    sleep 1

    # Check that the range is gone from node 1.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure

    # Check that it is now available on node 2.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
}
