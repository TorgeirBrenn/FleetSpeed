# FleetSpeed
![alt text](/doc_assets/FleetSpeed.png "FleetSpeed in action.")

## Prerequisites
BarentsWatch client id and secret in a `.env` file.

## Build and run, rust
```
cargo build
cargo run
```
The rust functionality is incomplete and not used. It was implemented for learning purposes. It can be run 
 and will then, for each second, display the number of AIS messages received the preceeding 10 seconds to `stdout`. 
The actual speed ranking is done in python using polars (which is all rust anyway).

## Build and run, python
```
poetry lock; poetry install
poetry run python pysrc/main.py
```