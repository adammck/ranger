setup_file() {
    go build
}

setup() {
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'

    ./kv -addr ":8001" >/dev/null 2>&1 &
    PID_8001=$!

    ./kv -addr ":8002" >/dev/null 2>&1 &
    PID_8002=$!

    # TODO: block until they're both started up, somehow
    sleep 0.5
}

teardown() {
    # TODO: do something sensible if the pids are invalid
    kill $PID_8001
    kill $PID_8002
}

@test "no valid range" {
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}
