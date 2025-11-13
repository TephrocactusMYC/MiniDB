# How to Test

To run unit tests or integration tests, you can use:

```shell
cargo test
```

and

```shell
cargo test -- --ignored
```

All tests should pass.

To run the client, follow these steps:

Open two terminals (or use **tmux**). In one terminal, run:

```shell
./debug-server.sh
```

This method is recommended because it allows you to see some logs.

In the other terminal, run:

```shell
./client.sh
```

Then enter the following commands one by one:

```
\r testdb

\c testdb

CREATE TABLE test (a INT, b INT, primary key (a));

\i ./data.csv test

SELECT a FROM test;

SELECT sum (a), sum(b) FROM test;

\shutdown
```

If everything works correctly, the output should match expectations, and you should not see any errors.

![](./CrustyDB.png)
