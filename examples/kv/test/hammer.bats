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

@test "hammer" {
    start_node 8001
    start_node 8002
    start_node 8003
    start_proxy 8000
    start_controller 9000

    sleep 0.5

    go run tools/hammer/main.go -addr localhost:8000 -workers 10 -interval 100 -count 1000
}
