# Build

```shell 
docker build -t tercen/gates_plotters_operator:latest .
docker push tercen/gates_plotters_operator:latest

git tag 0.2.15
docker build -t tercen/gates_plotters_operator:0.2.15 .
docker push tercen/gates_plotters_operator:0.2.15


docker inspect -f "{{ .Size }}" tercen/gates_plotters_operator:latest
```

```shell
cargo build --release
```


# Format
```shell
cargo fmt --all
```

# Test

```shell
cargo run --package gates_plotters_operator --bin gates_plotters_operator -- --taskId b587089d2031dd77d7b27e63843c18f0
```

# Deployment


