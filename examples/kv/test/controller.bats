setup_file() {
    go build
}

setup() {
    # TODO: Use this instead: https://github.com/ztombol/bats-docs#homebrew
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'
    load test_helper
    start_consul
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
    sleep 0.5

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
    run $(ranger_client 9000) move 1 8002
    assert_success

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
    run $(ranger_client 9000) split 1 b64:"$b" 8002 8003
    assert_success

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

@test "move across crash" {
    start_node 8001
    start_node 8002
    start_controller 9000
    sleep 0.5

    # Move the range from node 1 to node 2.
    run $(ranger_client 9000) move 1 8002
    assert_success

    crash_cmd 9000
    start_controller 9000
    sleep 0.5

    # Move the range from node 2 to node 1.
    run $(ranger_client 9000) move 1 8001
    assert_success

    # Check that the range is available on node 1, and not on node 2.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
}
