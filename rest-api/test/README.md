# Tests for the API server

These tests can be run with:

```Shell
./run_tests.sh -g ${sdk_dir}
```

You can optionally only run the unit tests or client tests with
```Shell
./run_tests.sh -g ${sdk_dir} unit
```
or
```Shell
./run_tests.sh -g ${sdk_dir} client
```

## Directory Structure

The tests are split into two directories:

`unit_test` is for unit_tests.  That is tests that can be run by themselves.

`client_test` are tests that require an instance of the API to be running.
