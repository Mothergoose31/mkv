# MKV - Distributed Key-Value Store

MKV  simple distributed key-value store with support for replication, multiple machines, and multiple drives per machine.

## Architecture

MKV consists of two main components:

1. **Master Server** - Handles the API and maintains the index of keys to volume locations
2. **Volume Servers** - Store the actual data (implemented using nginx)

MKV uses RocksDB for indexing and nginx for blob storage. The index can be reconstructed with the rebuild command, and volumes can be added or removed with the rebalance command.

## Getting Started

### Prerequisites

- Elixir 1.18 or later
- Erlang/OTP 25 or later
- nginx (for volume servers)

### Installation

1. Clone the repository
2. Run `mix deps.get` to install dependencies
3. Make the scripts executable:
   ```
   chmod +x bin/mkv
   chmod +x bin/volume
   ```

### Running MKV

1. Start Volume Servers (default port 3001)
   ```
   PORT=3001 ./bin/volume /tmp/volume1/ &
   PORT=3002 ./bin/volume /tmp/volume2/ &
   PORT=3003 ./bin/volume /tmp/volume3/ &
   ```

2. Start Master Server (default port 3000)
   ```
   ./bin/mkv server -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/
   ```

## API

### Basic Operations

- **GET /key** - 302 redirect to nginx volume server
- **PUT /key** - Blocks. 201 = written, anything else = probably not written
- **DELETE /key** - Blocks. 204 = deleted, anything else = probably not deleted
- **UNLINK /key** - Virtual delete, marks the key as deleted without removing the data

### Additional Operations

- **LIST /prefix?list** - List keys with the given prefix
- **LIST /?unlinked** - List unlinked keys (virtually deleted)

## Usage Examples

```bash

curl -v -L -X PUT -d bigswag localhost:3000/wehave

curl -v -L localhost:3000/wehave

curl -v -L -X DELETE localhost:3000/wehave

curl -v -L -X UNLINK localhost:3000/wehave

curl -v -L localhost:3000/we?list

curl -v -L localhost:3000/?unlinked

curl -v -L -X PUT -T /path/to/local/file.txt localhost:3000/file.txt

curl -v -L -o /path/to/local/file.txt localhost:3000/file.txt
```

## Command Line Arguments

```
Usage: ./bin/mkv <server, rebuild, rebalance>

  -db string        Path to RocksDB
  -fallback string  Fallback server for missing keys
  -port int         Port for the server to listen on (default 3000)
  -protect          Force UNLINK before DELETE
  -replicas int     Amount of replicas to make of the data (default 3)
  -subvolumes int   Amount of subvolumes, disks per machine (default 10)
  -volumes string   Volumes to use for storage, comma separated
```

## Operations

### Rebalancing (to change the amount of volume servers)

```bash
# NEEDS TO BE SHUT DOWN  ROCKSDB ONLY HANDLES ONE OPERATION
./bin/mkv rebalance -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/
```

### Rebuilding (to regenerate the RocksDB)

```bash
./bin/mkv rebuild -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdbalt/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

