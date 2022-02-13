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

    # Check that the range is gone from node 1.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure

    # Check that it is now available on node 2.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
}

@test "split" {
    start_node 8001
    start_node 8002
    start_node 8003
    start_controller 9000

    sleep 0.5

    # Write a key on either side of the split.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$c'", "value": "'$yyy'"}'
    assert_success

    # Split the range from node 1 to nodes 2 and three.
    run bin/client.sh 9000 ranger.Balancer.Split '{"range": {"key": 1}, "boundary": "'$b'", "node_left": "8002", "node_right": "8003"}'

    # Check that the range is gone from node 1.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure

    # Check that the left side is available on node 1, but the right is not.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$c'", "value": "'$zzz'"}'
    assert_failure

    # Check that the right side is available on node 2, but the left is not.
    run bin/client.sh 8003 kv.KV.Put '{"key": "'$c'", "value": "'$zzz'"}'
    assert_success
    run bin/client.sh 8003 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
}
