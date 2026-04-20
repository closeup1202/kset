# kset

Apache Kafka setup CLI — interactive installer for local and Docker environments.

## Install

```bash
npm install -g @closeup1202/kset
```

Or use without installing:

```bash
npx @closeup1202/kset init
```

## Commands

### `kset init`

Interactive wizard to set up Apache Kafka.

```bash
kset init
```

Walks you through:

- Environment selection (local or Docker)
- Kafka version (fetched dynamically from Apache)
- Mode (KRaft or Zookeeper)
- Broker count (1 / 3 / 5)
- Replication factor
- Port
- Initial topic creation

**Local install** downloads the official Apache Kafka tarball, applies your config, runs KRaft storage format, and optionally creates an initial topic — all in one command.

**Docker** generates a ready-to-use `docker-compose.yml` (and `server.properties` for single broker).

### `kset check`

Check system requirements before installation.

```bash
kset check
kset check --port 9094
```

Checks:

- Java version (11+ required, 17+ recommended)
- Docker and Docker Compose
- Port availability

## Supported Environments

| Environment | Broker Count | KRaft | Zookeeper |
|-------------|-------------|-------|-----------|
| Local       | 1, 3, 5     | ✅    | ✅ (3.x)  |
| Docker      | 1, 3, 5     | ✅    | ✅ (3.x)  |

## Supported Kafka Versions

Versions are fetched dynamically from [downloads.apache.org/kafka](https://downloads.apache.org/kafka/). Any available version is supported.

KRaft mode is available from **3.3.0+**. Kafka **4.0+** uses KRaft only.

## License

MIT