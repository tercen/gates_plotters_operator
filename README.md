

```shell 
docker build -t tercen/gates_plotters_operator:latest .
docker push tercen/gates_plotters_operator:latest

docker build -t tercen/gates_plotters_operator:0.1.1 .
docker push tercen/gates_plotters_operator:0.1.1

```

```shell
cargo build --release
```


# Format
```shell
cargo fmt --all
```