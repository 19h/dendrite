[← Documentation home](../../README.md)

# Playbook: run Dendrite with Docker

This playbook builds the checked-in multi-stage image, persists state and
payloads in named volumes, keeps the administrator API inside the container, and
uses the bundled `dendritectl` through `docker exec`.

The current Dockerfile pins `1.94.0-x86_64-unknown-linux-gnu`; treat this as the
documented Linux/x86-64 container path.

## 1. Build the image

From the repository root:

```sh
docker build --file packaging/Dockerfile --tag dendrite:local .
```

The builder compiles locked release binaries for `dendrite` and `dendritectl`.
The runtime image is Debian Bookworm, runs as UID 10001, and uses
`/home/dendrite` as its working directory.

## 2. Create persistent volumes and an import directory

```sh
docker volume create dendrite-data
docker volume create dendrite-downloads
mkdir -p imports
```

Place `.torrent` inputs in `./imports`. That directory will be mounted read-only;
the daemon writes payloads only to its downloads volume.

## 3. Start the container

```sh
docker run --detach \
  --name dendrite \
  --mount source=dendrite-data,target=/home/dendrite/dendrite-data \
  --mount source=dendrite-downloads,target=/home/dendrite/downloads \
  --mount type=bind,src="$(pwd)/imports",dst=/imports,readonly \
  --publish 16493:16493/tcp \
  --publish 16493:16493/udp \
  --publish 16309:16309/udp \
  dendrite:local
```

Checkpoint:

```sh
docker ps --filter name=dendrite
docker logs dendrite
```

The default API binds to container loopback. Do not publish port 8412 and expect
it to work: Docker port forwarding targets the container interface, while the
daemon is intentionally listening only on `127.0.0.1` inside that network
namespace.

## 4. Administer inside the container

```sh
docker exec dendrite \
  dendritectl \
  --token-file /home/dendrite/dendrite-data/admin.token \
  status
```

This keeps both the API request and token inside the container. Import a mounted
file:

```sh
docker exec dendrite \
  dendritectl \
  --token-file /home/dendrite/dendrite-data/admin.token \
  add /imports/example.torrent --start
```

A quoted magnet needs no import mount:

```sh
docker exec dendrite \
  dendritectl \
  --token-file /home/dendrite/dendrite-data/admin.token \
  add 'magnet:?xt=urn:btih:…' --start
```

## 5. Inspect and stop

```sh
docker exec dendrite dendritectl list
docker logs --follow dendrite
docker stop --time 35 dendrite
```

The stop timeout accommodates the daemon's 30-second engine shutdown grace. The
named volumes survive container removal:

```sh
docker rm dendrite
```

Recreate the container with the same mounts to restore starting, downloading,
and checking records. Stopped, seeding, and error records remain durable but do
not automatically create a new download actor.

Removing `dendrite-data` loses the service database and token. Removing
`dendrite-downloads` deletes payload data. Volume deletion is intentionally not
part of the normal cleanup commands.

## Configure the container

For custom settings, mount a read-only TOML file and pass its path to the image
entrypoint:

```sh
docker run --detach \
  --name dendrite \
  --mount source=dendrite-data,target=/home/dendrite/dendrite-data \
  --mount source=dendrite-downloads,target=/home/dendrite/downloads \
  --mount type=bind,src="$(pwd)/dendrite.toml",dst=/etc/dendrite.toml,readonly \
  --publish 16493:16493/tcp \
  --publish 16493:16493/udp \
  --publish 16309:16309/udp \
  dendrite:local --config /etc/dendrite.toml
```

Paths inside TOML are container paths, not host paths. Keep the documented volume
targets aligned with `data_dir` and `download_dir`.

## Expose the API only deliberately

To access port 8412 from outside the container, configure a non-loopback API
listener, mount TLS certificate/key files, set exact `allowed_origins`, and then
publish the port. Dendrite rejects a remote binding without those safeguards.
Follow the [remote API playbook](remote-api.md); do not weaken the listener to
plain HTTP for convenience.

## Failure branches

- **Container exits immediately:** inspect `docker logs dendrite`; configuration,
  volume permissions, and listener conflicts fail startup.
- **Client cannot read the token:** use the absolute container token path shown
  above and confirm the data volume is mounted.
- **Host client cannot reach 8412:** expected under local defaults; use
  `docker exec` or configure the remote API correctly.
- **No incoming peers:** publish port 16493 for both TCP and UDP and check the
  host firewall/NAT policy.
- **DHT does not find peers:** configure bootstrap nodes; publishing the DHT port
  does not populate `dht_bootstrap`.

## Next steps

- Container-safe settings: [Configuration guide](../getting-started/configuration.md)
- Secure external control: [Remote API](remote-api.md)
- Persistent files: [Data layout](../reference/data-layout.md)
