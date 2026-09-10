# Object storage

Latu never opens your bucket. A read is a plan with a path in it, Spark opens the object, and
Arrow comes back — so anything the cluster can read, Latu can read, and the client-side change
is one string.

That leaves the interesting half on the server, which is what this page is about.

## The two halves

**On the server**: the S3A filesystem has to be on the classpath, and it needs an endpoint, a
region and credentials.

**In Latu**: `path: "s3a://bucket/key"`.

`Latu.add_jar/3` is not a way to do the first half. It reaches the driver's classloader, which
is enough for a UDF and not enough for a filesystem — every JVM that opens an object needs the
classes, so the jars belong on the cluster's own classpath.

## A store to talk to

The repo's `docker-compose.yml` carries MinIO and a Spark Connect server with `hadoop-aws`
on it, behind a profile — a bare `up -d` leaves them alone, because they cost two images and
several hundred megabytes of jars that most work here never needs:

```bash
docker compose --profile s3 up -d --wait
```

MinIO on :9000 with an empty bucket `latu`, and Spark Connect on **:15004**. The first start
resolves `hadoop-aws` and the AWS SDK bundle from Maven Central — a few hundred megabytes, and
minutes rather than seconds. It is cached in a volume after that, so only `down -v` pays it
again. A machine with no route to Maven Central wants the jars dropped into `/opt/spark/jars`
instead.

The version is the one thing that is not free to choose: `hadoop-aws` must match the Hadoop the
image was built against.

```bash
docker exec latu-spark-s3 ls /opt/spark/jars | grep hadoop-client
```

Two lines, both `3.5.0` on `apache/spark:4.2.0`. A mismatch is a `NoSuchMethodError` at the
first read rather than an honest refusal at startup.

## What S3A needs told

| key | this compose | why |
| --- | --- | --- |
| `fs.s3a.endpoint` | `http://minio:9000` | The **server** resolves this, so it is the compose network name, not `localhost`. A scheme decides TLS; with none, `fs.s3a.connection.ssl.enabled` does. |
| `fs.s3a.endpoint.region` | `us-east-1` | V4 signing wants a region and a third-party store does not care which. Anything except `sdk`, `ec2` and `auto` — Hadoop reserves those three. |
| `fs.s3a.path.style.access` | `true` | Bucket in the path rather than the hostname. MinIO has no per-bucket DNS. |
| `fs.s3a.access.key`, `fs.s3a.secret.key` | `latu`, `latu-secret-key` | The default provider chain already tries these, so there is no provider class to name. |

One more is worth knowing rather than copying. Hadoop 3.5 changed the default reader to the AWS
analytics accelerator, which is tuned for S3 itself; `fs.s3a.input.stream.type=classic` is the
long-standing one and the first thing to put back when a third-party store reads oddly.

## Getting them there from Elixir

The usual advice — `spark.hadoop.*` on the submit line — is a start-up setting, and a Connect
client has no start-up to hook. Two things work from here instead, both because Spark builds
the Hadoop configuration for a data source out of the session rather than out of the JVM.

**Session confs, under their raw keys.** Spark copies every SQL conf into that configuration
as-is, so the key is `fs.s3a.endpoint`, with no `spark.hadoop.` in front of it. Unknown keys
are not an error — `Latu.set_confs/2`'s own docs say so, and this is the reason.

**Reader options.** Spark folds a data source's options into the same configuration, minus
`path` and `paths`. That scopes credentials to one frame, which is how one session reads two
stores. These keys have dots in them, so they must be written as **strings**: `Latu.read/2`
camel-cases an atom and passes a string verbatim.

## The round trip

Everything above is the server's business, so the fences here run against the ordinary test
server on :15002 and prove only the shape — a path is a string, and Parquet in and out of it
does not care what the string names. The two marked below are run by `mix test --include s3`,
against the stack, which is the one command `mix check.all` leaves to you.

```elixir
{:ok, session} = Latu.connect("sc://localhost:15002")
```

```elixir
{:ok, people} =
  Latu.create_dataframe(session, [
    %{name: "Ada", city: "London"},
    %{name: "Grace", city: "New York"}
  ])

local = "/tmp/latu_object_storage"

:ok = Latu.write(people, format: "parquet", path: local, mode: :overwrite)

{:ok, 2} = session |> Latu.read(format: "parquet", path: local) |> Latu.count()
```

Against the S3 stack it is the same calls with the confs set first, and the only line that
changed is the path.

> **Not executed.** Not up in `mix check.all`: the s3 profile. `mix test --include s3` runs them.

```elixir
{:ok, s3} = Latu.connect("sc://localhost:15004")

:ok =
  Latu.set_confs(s3, %{
    "fs.s3a.endpoint" => "http://minio:9000",
    "fs.s3a.endpoint.region" => "us-east-1",
    "fs.s3a.path.style.access" => "true",
    "fs.s3a.access.key" => "latu",
    "fs.s3a.secret.key" => "latu-secret-key",
    "fs.s3a.input.stream.type" => "classic"
  })

{:ok, uploaded} =
  Latu.create_dataframe(s3, [
    %{name: "Ada", city: "London"},
    %{name: "Grace", city: "New York"}
  ])

:ok = Latu.write(uploaded, format: "parquet", path: "s3a://latu/people", mode: :overwrite)

{:ok, 2} = s3 |> Latu.read(format: "parquet", path: "s3a://latu/people") |> Latu.count()
```

The reader-option form needs nothing set on the session at all. Credentials live and die with
the frame, and a second bucket elsewhere is a second list.

> **Not executed.** Not up in `mix check.all`: the s3 profile. `mix test --include s3` runs them.

```elixir
{:ok, plain} = Latu.connect("sc://localhost:15004")

{:ok, rows} =
  plain
  |> Latu.read([
    {"fs.s3a.endpoint", "http://minio:9000"},
    {"fs.s3a.endpoint.region", "us-east-1"},
    {"fs.s3a.path.style.access", "true"},
    {"fs.s3a.access.key", "latu"},
    {"fs.s3a.secret.key", "latu-secret-key"},
    format: "parquet",
    path: "s3a://latu/people"
  ])
  |> Latu.collect()

["Ada", "Grace"] = rows |> Enum.map(& &1.name) |> Enum.sort()
```

## When it does not work

`UnknownHostException` on `latu.minio` is path-style access not taking. A hang of tens of
seconds before a credentials error is the SDK looking for an instance profile, which means the
region or the keys never arrived — read them back with `Latu.confs!(s3, prefix: "fs.s3a.")`
before blaming the store. `403` with the keys plainly right is usually the container clock
having drifted far enough to break signing.

MinIO's own view is the quickest second opinion. The image's built-in `local` alias carries
the default root credentials, which this compose replaces, so hand `mc` its own — otherwise
the answer is `Access Denied` and it is `mc` being refused, not you:

```bash
docker exec -e MC_HOST_s3=http://latu:latu-secret-key@localhost:9000 latu-minio \
  mc ls --recursive s3/latu
```

An empty bucket prints nothing, which is not the same as a missing one. `mc ls s3/` says
which.

## Somewhere that isn't MinIO

**AWS S3** needs the least of any of them: no endpoint, no path-style, one region, and the
reader Hadoop 3.5 now defaults to is the one written for it. Credentials are the only real
decision. The default chain tries four things in order — session credentials
(`fs.s3a.session.token` beside the key pair), a long-lived key pair, the environment, an
instance profile — so a cluster with a role attached wants nothing set at all, and STS
credentials for an assumed role are three session confs that expire by themselves. Prefer
either to a permanent key.

**Cloudflare R2** is MinIO's shape with a real endpoint —
`https://<account>.r2.cloudflarestorage.com`, path-style on, keys as above. Its region is the
one trap: `auto` is what every R2 tutorial says and one of the three names Hadoop refuses, so
pick a literal like `us-east-1`.

**Two stores at once.** Every one of these keys has a per-bucket form,
`fs.s3a.bucket.<bucket>.endpoint` and so on, expanded when S3A opens that bucket. Once a
session talks to more than one place, that beats the reader-option list above.

None of this is only tidiness. A key set as a session conf crosses the gRPC channel in
whatever the channel is — `sc://` is cleartext until you ask for TLS — and stays readable on
that session afterwards. A path is public knowledge; a secret key is not, and the least bad
place for one is a role the cluster already has.
