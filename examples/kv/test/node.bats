setup_file() {
    go build
}

setup() {
    # TODO: Use this instead: https://github.com/ztombol/bats-docs#homebrew
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'
    load test_helper

    # TODO: This should not be necessary for a node to start. Service discovery
    # should be best-effort and the nodes should start up anyway in case they
    # have stale data to serve.
    start_consul
}

teardown() {
    stop_cmds
}

@test "read write to unassigned range" {
    start_node 8001

    # Try to read a key from node 1 which is not assigned.
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Try to write same.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}

@test "assign range" {
    start_node 8001

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Prepare '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to read again. Different error.
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: NotFound'
    assert_line -n 2 '  Message: no such key'

    # Try to write again. Success!
    zzz=$(echo -n zzz | base64)
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success

    # Read again. Success!
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'
}

@test "dump range" {
    start_node 8001

    # Dump a range which doesn't exist.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: InvalidArgument'
    assert_line -n 2 '  Message: range not found'

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Prepare '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to dump the contents of range 1 from node 1.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only dump ranges in the TAKEN state'

    # Deactivate the range from node 1.
    run bin/client.sh 8001 ranger.Node.Deactivate '{"range": {"key": 1}}'
    assert_success

    # Try to dump again. Success!
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'
}

@test "move range" {
    start_node 8001
    start_node 8002

    # ---- setup

    # Prepare the range [a,b) on node 1, Put some keys, and Deactivate it.
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}'
    bin/client.sh 8001 ranger.Node.Prepare '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    bin/client.sh 8001 ranger.Node.Deactivate '{"range": {"key": 1}}'

    # ---- test

    # Move that range to node 2
    # TODO: This succeeds even if node 1 refuses to dump it, because the
    #       transfer starts asynchronously after this rpc has returned. That
    #       doesn't seem ideal.
    run bin/client.sh 8002 ranger.Node.Prepare '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # Read the key from node 1. This still works!
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Try to write to node 1. This no longer works.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only PUT to ranges in the READY state'

    # Read the key from node 2. This works now!
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Try to write to node 2. This doesn't work yet.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
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
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Enable writes on node 2. This works.
    # This would have worked at any time after node 2 finished fetching the
    # range from node 1, but one mustn't enable writes until the old node has
    # stopped serving reads, to avoid stale reads.
    run bin/client.sh 8002 ranger.Node.Activate '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Try to write to node 2. Success!
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Read the new value back. Success!
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$yyy'"'
    assert_line -n 2 '}'
}

@test "join ranges" {
    start_node 8001
    start_node 8002
    start_node 8003

    # ---- setup

    # Assign a couple of adjacent ranges, write some keys, and TAKE them to
    # prepare for moving.

    # Node 1: range [a,b)
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}'
    bin/client.sh 8001 ranger.Node.Prepare '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a1'", "value": "'$zzz'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a2'", "value": "'$yyy'"}'
    bin/client.sh 8001 ranger.Node.Deactivate '{"range": {"key": 1}}'

    # Node 2: range [b,c)
    r2='{"ident": {"key": 2}, "start": "'$b'", "end": "'$c'"}'
    bin/client.sh 8002 ranger.Node.Prepare '{"range": '"$r2"'}'
    bin/client.sh 8002 kv.KV.Put '{"key": "'$b1'", "value": "'$xxx'"}'
    bin/client.sh 8002 kv.KV.Put '{"key": "'$b2'", "value": "'$www'"}'
    bin/client.sh 8002 ranger.Node.Deactivate '{"range": {"key": 2}}'

    # ---- test

    # TODO: Test joining a range where one of the parents is already on the
    #       destination node. Think it should Just Work but things might get
    #       weird when serving reads during the transition.

    # Move both ranges to a single new range on node 3
    run bin/client.sh 8003 ranger.Node.Prepare '{"range": {"ident": {"key": 3}, "start": "'$a'", "end": "'$c'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}, {"range": '"$r2"', "node": "localhost:8002"}]}'
    assert_success

    # TODO: Can be arbitrary delay here because Prepare returns before range
    #       recovery is finished (or even started). Need some rpc to wait for
    #       the recovery to succeed or fail. Probably just for testing.
    sleep 0.5

    # Read a key that was on node 1
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$a2'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$yyy'"'
    assert_line -n 2 '}'

    # Read a key that was on node 2
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$b1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$xxx'"'
    assert_line -n 2 '}'
}

@test "split ranges" {
    start_node 8001
    start_node 8002
    start_node 8003

    # ---- setup

    # Assign a range and write some keys.                                                                                                                                                                                                                                                                             
    # Node 1: range [a,c)
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$c'"}'
    bin/client.sh 8001 ranger.Node.Prepare '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a1'", "value": "'$zzz'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a2'", "value": "'$yyy'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$b1'", "value": "'$xxx'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$b2'", "value": "'$www'"}'
    bin/client.sh 8001 ranger.Node.Deactivate '{"range": {"key": 1}}'

    # ---- test

    # Move half of the range to n2
    run bin/client.sh 8002 ranger.Node.Prepare '{"range": {"ident": {"key": 2}, "start": "'$a'", "end": "'$b'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # Move the other half to n3
    run bin/client.sh 8003 ranger.Node.Prepare '{"range": {"ident": {"key": 3}, "start": "'$b'", "end": "'$c'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # TODO: Can be arbitrary delay here because Prepare returns before range
    #       recovery is finished (or even started). Need some rpc to wait for
    #       the recovery to succeed or fail. Probably just for testing.
    sleep 0.5

    # Read a key that should have made it to n2 / [a,b)
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Read a key that was in the parent range, but should NOT have made it to n2
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$b1'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Read a key that should have made it to n3 / [b,c)
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$b1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$xxx'"'
    assert_line -n 2 '}'

    # Read a key that was in the parent range, but should NOT have made it to n3
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$a1'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}
