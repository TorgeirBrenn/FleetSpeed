## Build and run, rust
```
cargo build
cargo run
```
This will stream the AIS messages to `stdout`. Currently, there is no more functionality as this was purely a learning
exercise. The actual speed ranking is done in python using polars (which is all rust anyway).

## Build and run, python (WIP)
```
pip install maturin
maturing build --release
poetry install
```