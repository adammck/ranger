setup_file() {
    go build
}

setup() {
    # TODO: I don't want to vendor bats, but do something less dumb than this
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'

    ./kv -addr ":8001" >$BATS_TMPDIR/kv-8001.log 2>&1 &
    PID_8001=$!

    ./kv -addr ":8002" >$BATS_TMPDIR/kv-8002.log 2>&1 &
    PID_8002=$!

    ./kv -addr ":8003" >$BATS_TMPDIR/kv-8003.log 2>&1 &
    PID_8003=$!

    # TODO: Move wait-port tool into a func in this file.
    wait-port 8001
    wait-port 8002
    wait-port 8003
}

teardown() {
    # TODO: do something sensible if the pids are invalid
    kill $PID_8001
    kill $PID_8002
    kill $PID_8003
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

@test "join ranges" {
    a=$(echo -n a | base64)
    b=$(echo -n b | base64)
    c=$(echo -n c | base64)
    aaa=$(echo -n aaa | base64)
    bbb=$(echo -n bbb | base64)
    ccc=$(echo -n ccc | base64)
    ddd=$(echo -n ddd | base64)

    # ---- setup

    # Assign a couple of adjacent ranges, write some keys, and TAKE them to
    # prepare for moving.

    # Node 1: range [a,b)
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}'
    bin/client.sh 8001 ranger.Node.Give '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "a1", "value": "'$aaa'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "a2", "value": "'$bbb'"}'
    bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'

    # Node 2: range [b,c)
    r2='{"ident": {"key": 2}, "start": "'$b'", "end": "'$c'"}'
    bin/client.sh 8002 ranger.Node.Give '{"range": '"$r2"'}'
    bin/client.sh 8002 kv.KV.Put '{"key": "b1", "value": "'$ccc'"}'
    bin/client.sh 8002 kv.KV.Put '{"key": "b2", "value": "'$ddd'"}'
    bin/client.sh 8002 ranger.Node.Take '{"range": {"key": 2}}'

    # ---- test

    # Move both ranges to a single new range on node 3
    run bin/client.sh 8003 ranger.Node.Give '{"range": {"ident": {"key": 3}, "start": "'$a'", "end": "'$c'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}, {"range": '"$r2"', "node": "localhost:8002"}]}'
    assert_success

    # TODO: Can be arbitrary delay here because Give returns before range
    #       recovery is finished (or even started). Need some rpc to wait for
    #       the recovery to succeed or fail. Probably just for testing.
    sleep 0.5

    # Read a key that was on node 1
    run bin/client.sh 8003 kv.KV.Get '{"key": "a2"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$bbb'"'
    assert_line -n 2 '}'

    # Read a key that was on node 2
    run bin/client.sh 8003 kv.KV.Get '{"key": "b1"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$ccc'"'
    assert_line -n 2 '}'
}
