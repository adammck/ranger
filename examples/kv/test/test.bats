setup_file() {
    go build
}

setup() {
    # TODO: I don't want to vendor bats, but do something less dumb than this
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'

    ./kv -addr ":8001" >/dev/null 2>&1 &
    PID_8001=$!

    ./kv -addr ":8002" >/dev/null 2>&1 &
    PID_8002=$!

    # TODO: Move wait-port tool into a func in this file.
    wait-port 8001
    wait-port 8002
}

teardown() {
    # TODO: do something sensible if the pids are invalid
    kill $PID_8001
    kill $PID_8002
}

@test "read write to unassigned range" {
    a=$(echo -n a | base64)
    aaa=$(echo -n aaa | base64)

    # Try to read a key from node 1 which is not assigned.
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Try to write same.
    run bin/client.sh 8001 kv.KV.Put '{"key": "a", "value": "'$aaa'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}

@test "assign range" {
    a=$(echo -n a | base64)
    b=$(echo -n b | base64)

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to read again. Different error.
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: NotFound'
    assert_line -n 2 '  Message: no such key'

    # Try to write again. Success!
    aaa=$(echo -n aaa | base64)
    run bin/client.sh 8001 kv.KV.Put '{"key": "a", "value": "'$aaa'"}'
    assert_success

    # Read again. Success!
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$aaa'"'
    assert_line -n 2 '}'
}

@test "dump range" {
    a=$(echo -n a | base64)
    b=$(echo -n b | base64)

    # Dump a range which doesn't exist.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: InvalidArgument'
    assert_line -n 2 '  Message: range not found'

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to dump the contents of range 1 from node 1.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only dump ranges in the TAKEN state'

    # Take the range from node 1.
    run bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'
    assert_success

    # Try to dump again. Success!
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'
}

@test "move range" {
    a=$(echo -n a | base64)
    b=$(echo -n b | base64)
    aaa=$(echo -n aaa | base64)
    bbb=$(echo -n bbb | base64)

    # ---- setup

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Write a key to node 1.
    run bin/client.sh 8001 kv.KV.Put '{"key": "a", "value": "'$aaa'"}'
    assert_success

    # Take the range from node 1, so it can be given to node 2.
    run bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'
    assert_success

    # ---- test

    # Move that range to node 2
    # TODO: This succeeds even if node 1 refuses to dump it, because the
    #       transfer starts asynchronously after this rpc has returned. That
    #       doesn't seem ideal.
    run bin/client.sh 8002 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}, "source": "localhost:8001"}'
    assert_success

    # Read the key from node 1. This still works!
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$aaa'"'
    assert_line -n 2 '}'

    # Try to write to node 1. This no longer works.
    run bin/client.sh 8001 kv.KV.Put '{"key": "a", "value": "'$bbb'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only PUT to ranges in the READY state'

    # Read the key from node 2. This works now!
    run bin/client.sh 8002 kv.KV.Get '{"key": "a"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$aaa'"'
    assert_line -n 2 '}'

    # Try to write to node 2. This doesn't work yet.
    run bin/client.sh 8002 kv.KV.Put '{"key": "a", "value": "'$bbb'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only PUT to ranges in the READY state'

    # Drop the range from node 1.
    run bin/client.sh 8001 ranger.Node.Drop '{"range": {"key": 1}}'
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Try to read from node 1. No longer works. The range is gone.
    run bin/client.sh 8001 kv.KV.Get '{"key": "a"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Enable writes on node 2. This works.
    # This would have worked at any time after node 2 finished fetching the
    # range from node 1, but one mustn't enable writes until the old node has
    # stopped serving reads, to avoid stale reads.
    run bin/client.sh 8002 ranger.Node.Serve '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Try to write to node 2. Success!
    run bin/client.sh 8002 kv.KV.Put '{"key": "a", "value": "'$bbb'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Read the new value back. Success!
    run bin/client.sh 8002 kv.KV.Get '{"key": "a"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$bbb'"'
    assert_line -n 2 '}'
}
