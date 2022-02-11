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

@test "range assignment" {
    start_node 8001

    # Try to write something to the node. This should fail, because no ranges
    # are assigned.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    start_controller 9000
    sleep 1 # lol

    # Try the same write again. It should succeed this time, because the
    # controller has assigned the first (infinite) range to it.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success
}
