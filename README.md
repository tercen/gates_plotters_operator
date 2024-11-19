

```shell 
docker build -t tercen/gates_plotters_operator:latest .
docker push tercen/gates_plotters_operator:latest

git tag 0.2.0
docker build -t tercen/gates_plotters_operator:0.2.0 .
docker push tercen/gates_plotters_operator:0.2.0


docker inspect -f "{{ .Size }}" tercen/gates_plotters_operator:latest
```

```shell
cargo build --release
```


# Format
```shell
cargo fmt --all
```

```shell
run --package gates_plotters_operator --bin gates_plotters_operator -- --taskId e4c6293e21e71057806d192e43e53ebe```