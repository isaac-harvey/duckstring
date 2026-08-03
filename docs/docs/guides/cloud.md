---
title: Running on AWS
description: Object-store data, remote workers on Fargate or EC2, and the Flock — on your own AWS account.
---

# Running on AWS

A stock Catchment runs everything on one box: workers are subprocesses, data lands on local disk. Point it at object storage and give it somewhere to launch workers, and the same Catchment runs those workers on cloud compute instead — your account, your VPC, your IAM.

Nothing here is a different product or a different code path. A `pond.toml` that asks for a big worker runs fine on a laptop (the request degrades to a local subprocess); the same file on a cloud-configured Catchment gets the real thing.

Two things unlock it, and both must be true:

1. **A remote data root** — `s3://…`. Workers on other machines have to read and write somewhere everyone can see.
2. **AWS credentials the Catchment can use** — an instance role, `AWS_*` env vars, a profile, anything the boto3 chain finds.

Check with `duckstring catchment settings`:

```
data root:  s3://acme-lake/duckstring
AWS creds:  yes
cloud:      enabled
```

Until both hold, remote compute options are inert and every Pond runs locally. That is deliberate: a misconfigured cloud should slow you down, not break you.

## The shape of it

```
your laptop ── SSH tunnel ─┐
                           ▼
                    ┌─────────────┐        launches        ┌──────────────┐
                    │  Catchment  │ ─────────────────────► │ Fargate task │
                    │  (a box you │                        │  or EC2 box  │
                    │   run)      │ ◄───────────────────── │   (a Duck)   │
                    └─────────────┘      dials back        └──────────────┘
                           │                                      │
                           └──────────► s3://… ◄──────────────────┘
                                     (the data plane)
```

The important direction is the arrow back: **a worker always dials the Catchment**, never the reverse. So workers need no inbound access at all — only egress to the Catchment's port and to S3. Everything below follows from that.

## Step 1: the data root

```bash
duckstring catchment settings --data-root s3://acme-lake/duckstring
```

Set-once in practice: once a Pond has published, changing it would strand that data, so the setter refuses. Credentials can ride the URI as `${env:NAME}` / `${secret:NAME}` references, or come from the instance role — the latter is better, because nothing sensitive lands in config.

## Step 2: IAM

Three roles, and the two permissions everyone forgets are called out because we forgot them too.

**The Catchment's role** (its instance profile) launches workers and reads the bucket:

- `ecs:RunTask`, `StopTask`, `DescribeTasks`, `RegisterTaskDefinition`, `DescribeTaskDefinition`, **`ecs:TagResource`**
- `ec2:RunInstances`, `TerminateInstances`, `CreateTags`, `DescribeInstances` (only if you use EC2 workers)
- **`iam:PassRole`** for each role it hands to a worker — the single most-forgotten permission
- S3 read/write on the data bucket

:::warning `ecs:TagResource`
The launcher tags every task it starts, so without this `RunTask` is denied outright. The error is clear once you see it, but it only appears in the Pond's failure message.
:::

**The worker's role** — S3 read/write on the data bucket, and nothing else. Workers never receive the Catchment's credentials; they use their own instance/task role.

**The ECS execution role** (Fargate only) pulls the image and writes logs. Start from the AWS-managed `AmazonECSTaskExecutionRolePolicy`, then add log-group permissions:

```json
{
  "Effect": "Allow",
  "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
  "Resource": [
    "arn:aws:logs:REGION:ACCOUNT:log-group:/duckstring/duck",
    "arn:aws:logs:REGION:ACCOUNT:log-group:/duckstring/duck:*"
  ]
}
```

:::warning Both ARN forms
`CreateLogGroup` alone, or only the `:*` form, still fails — with `ResourceInitializationError: failed to validate logger args`, which reads like a configuration problem rather than a permissions one. You only need `CreateLogGroup` the first time; once the group exists a broken policy stops being visible, so this can pass for weeks and then fail on a fresh account.
:::

## Step 3: networking

Two security groups:

- **`sg-catchment`** — inbound `7474` **from `sg-duck`**, plus SSH from your own IP if you want it.
- **`sg-duck`** — no inbound rules at all. Egress only.

That asymmetry is the whole model: workers dial out, so they need nothing open. See [Debugging a worker](#debugging-a-worker) for how to get onto a box without changing that.

## Step 4a: Fargate workers (the default)

Fargate needs a container image and somewhere to run it:

```bash
DUCKSTRING_FARGATE_IMAGE=<account>.dkr.ecr.<region>.amazonaws.com/duckstring:latest
DUCKSTRING_FARGATE_CLUSTER=duckstring
DUCKSTRING_FARGATE_SUBNETS=subnet-…
DUCKSTRING_FARGATE_SECURITY_GROUPS=sg-duck
DUCKSTRING_FARGATE_EXECUTION_ROLE=arn:aws:iam::…:role/ecsTaskExecutionRole
DUCKSTRING_FARGATE_TASK_ROLE=arn:aws:iam::…:role/duckstring-duck
DUCKSTRING_FARGATE_ASSIGN_PUBLIC_IP=ENABLED
```

`ASSIGN_PUBLIC_IP=ENABLED` is the simple answer for a public subnet. In a private subnet use a NAT gateway or an S3 VPC endpoint instead — the task needs to reach S3 and the Catchment, not the internet as such.

Then put a Pond on it:

```bash
duckstring duck set sales --duck M          # a built-in preset: 1 vCPU / 4 GB
duckstring trigger pulse sales
```

`S`/`M`/`L`/`XL` are built-in Fargate presets — no pool to define. See [Building the image](#building-the-worker-image).

## Step 4b: EC2 workers

For sizes Fargate does not offer (beyond 16 vCPU / 120 GB), GPUs, or when you want a specific instance type:

```bash
DUCKSTRING_EC2_AMI=ami-…
DUCKSTRING_EC2_INSTANCE_PROFILE=duckstring-duck-ec2
DUCKSTRING_EC2_SUBNET=subnet-…
DUCKSTRING_EC2_SECURITY_GROUPS=sg-duck
DUCKSTRING_EC2_ASSIGN_PUBLIC_IP=ENABLED
```

```bash
duckstring duck pool add heavy --provider ec2 --instance-type m6i.4xlarge
duckstring duck set sales --duck heavy
```

:::warning The AMI needs a modern default `python3`
A worker boots with `pip3 install <spec>` then `python3 -m duckstring.duck`. So the image's **default** `python3` must be 3.10+ with a matching `pip3`. Stock Amazon Linux 2023 ships 3.9 and will never boot a worker — it exits silently, which is as confusing as it sounds.

Bake an image instead: launch a small instance, install python3.11 as the default `python3`, `pip install duckstring[aws]`, snapshot it. Use `--force-reinstall`, and before creating the image check `find /usr/local/lib/python3.11/site-packages -name '*.py' -size 0 | wc -l` returns `0` — an interrupted pip leaves zero-length files, and a later install considers the package already present and skips it. One truncated file produced a worker that started and exited 0 in complete silence.
:::

:::warning Subnet and security group are not optional
Omit them and AWS places the instance in the VPC's **default** security group, which normally cannot reach the Catchment. The worker boots, installs, and is then failed as unresponsive about a minute later — a networking problem that presents as a mysteriously dead worker.
:::

EC2 workers are slower to start (a minute or two versus about twenty seconds), which the Catchment allows for: a worker's *first* contact gets a provider-appropriate grace period before it is presumed dead.

## Building the worker image

You build and host it yourself. See [Choosing an image strategy](#choosing-an-image-strategy) — the short version is a private ECR repository in the same account and region:

```bash
python -m build                                   # produces dist/*.whl
docker build -t duckstring .                      # the repo's Dockerfile expects that wheel
aws ecr create-repository --repository-name duckstring
docker tag duckstring:latest <account>.dkr.ecr.<region>.amazonaws.com/duckstring:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/duckstring:latest
```

Build for the architecture you will run: `linux/amd64` unless you set `DUCKSTRING_FARGATE_CPU_ARCH=ARM64`. Building on an Apple-silicon laptop produces an arm64 image, which a default (x86) task will refuse.

### Choosing an image strategy

Duckstring does not publish a worker image for you to point at, and that is deliberate rather than
unfinished. A worker image is not a neutral artifact: it runs **in your account, with your data-plane
credentials**, and it needs to carry whatever your Ponds import. A single published image would be wrong
on all three counts — you could not verify its provenance, you could not add your dependencies, and its
release cadence would become yours.

So the model is: **you build it, you host it, you control what is in it.** Three ways, in the order most
people should consider them.

**1. Build from the published wheel (recommended).** The repo's `Dockerfile` installs a wheel you supply,
so you can pin exactly the version you run:

```dockerfile
FROM python:3.13-slim
RUN pip install "duckstring[aws]==0.4.0"
ENTRYPOINT ["python", "-m"]
CMD ["duckstring.duck", "--help"]
```

Push it to a **private ECR repository in the same account and region** as your workers. That is the
cheapest and fastest option — no cross-region data transfer, no registry credentials for ECS to manage,
and pulls are authorised by the execution role you already created.

**2. Add your own dependencies.** If your Ponds import anything beyond duckstring, extend the image —
this is the common case, and it is why a stock image would not have helped you anyway:

```dockerfile
FROM python:3.13-slim
RUN pip install "duckstring[aws]==0.4.0" pandas scikit-learn your-internal-lib
ENTRYPOINT ["python", "-m"]
```

A worker fetches its *Pond source* over the duck channel at boot, so the image only needs
**dependencies**, not your pipeline code. You rebuild it when your dependencies change, not when your
Ponds do.

**3. Skip images entirely.** EC2 workers boot from an AMI, not a container. If you already have
image-building for AMIs, or you want GPU drivers and instance-store setup, that path avoids Docker —
at the cost of a slower cold start and the AMI constraints described above.

Whichever you choose, match the architecture to `DUCKSTRING_FARGATE_CPU_ARCH` (`X86_64` by default,
`ARM64` for Graviton), and pin a version rather than tracking `:latest` — a worker image that changes
under you turns a dependency upgrade into an unannounced production change.

## The Flock: offloading big recomputes

When a Pond's full recompute would not fit the worker's memory, it can run the heavy scan-and-join on Athena while the worker keeps the incremental bookkeeping:

```bash
DUCKSTRING_FLOCK_ENGINE=athena
DUCKSTRING_FLOCK_ATHENA_WORKGROUP=duckstring-flock
DUCKSTRING_FLOCK_ATHENA_DATABASE=duckstring_flock
DUCKSTRING_FLOCK_ATHENA_SCRATCH=s3://acme-lake/flock-scratch
```

```bash
duckstring duck set priced --flock upgrade    # off | upgrade (default) | always
```

The worker's role also needs Athena and Glue permissions, plus `s3:GetBucketLocation` and `s3:ListBucketMultipartUploads` on the results bucket — without those Athena reports *"Unable to verify/create output bucket"* and the Flock quietly falls back to local compute. Give the scratch prefix a lifecycle rule; it is genuinely temporary.

**DuckDB is the authority.** A dispatch decides *where* work runs, never *what* gets published:

- Only expressions we have measured as equivalent are dispatched. Today: column references, comparisons, boolean logic, `+ - *`, `round`, `abs`, `floor`/`ceil`, `coalesce`. Division and `CAST` are **not** — Trino's `7 / 2` is `3`, DuckDB's is `3.5`, and Trino rounds `CAST(2.5 AS INTEGER)` to `3` where DuckDB gives `2`. Anything unproven runs locally.
- Whatever the engine returns is cast to the schema DuckDB would have produced, in DuckDB's column order. A result with different columns is rejected outright.
- A rejected or failed dispatch **falls back to local compute** — correct answers, just slower.

That last point has a consequence worth watching: a broken Flock is invisible in your data. Watch `duckstring_flock_dispatch_failures_total` on [`/metrics`](running-a-catchment.md#metrics), and the `flock_error` field on a Pond's status.

## Where data lives, and when

With cloud enabled, a Pond running on the Catchment's own box publishes **locally first** and mirrors to S3 in the background — the fast handoff for Ponds on the same box. A Pond on a remote worker publishes straight to S3.

That distinction is why a downstream Pond on a *different* machine waits for its source's mirror to land (`persisted_f`) rather than merely for the run to finish. It only ever delays a consumer; it never lets one read something that is not there yet.

A practical consequence: **stopping the Catchment does not wait for an in-flight mirror.** Nothing is lost or corrupted — on restart the Catchment reconciles what actually landed and re-runs the gap — but you can lose the last few seconds of work. Drain first if that matters:

```bash
duckstring do --all sleep     # stop new work, let in-flight runs finish
```

## Debugging a worker

Workers have no inbound access, so the usual reflex — SSH in — does not apply, and opening a hole is the wrong instinct: a worker box holds credentials for your whole data plane.

Three things that work without changing a rule:

**The Pond's failure message.** The Catchment asks the provider what happened and puts it in the failure: an ECS `stoppedReason`, the tail of the container's CloudWatch log, an EC2 instance state, or the tail of the boot console. Start here — it is usually enough.

**The serial console** (EC2). The boot script mirrors its output there, so a worker that dies before it ever connects still leaves a trace:

```bash
aws ec2 get-console-output --instance-id i-… --latest --output text | tail -40
```

**Session Manager** (EC2), for a box that is running but stuck. Attach `AmazonSSMManagedInstanceCore` to the worker role and you get an IAM-gated, audited shell with **no inbound rules at all**:

```bash
aws ssm start-session --target i-…
aws ssm send-command --instance-ids i-… --document-name AWS-RunShellScript \
  --parameters 'commands=["tail -50 /var/log/duckstring-boot.log"]'
```

Prefer that to an SSH rule. It cannot be left accidentally open to the internet, it is revocable centrally, and it does not break when your IP changes.

## Cost

You are paying for AWS, not for Duckstring:

- **The Catchment box** runs continuously — the bulk of a small deployment's cost. Stop it when idle; state survives on its volume and in S3.
- **Fargate and EC2 workers** are per-run. A worker is started when a Pond runs and stopped when it goes idle.
- **Athena** is per-terabyte scanned. Only reached when the Flock dispatches.
- **S3** — storage plus requests; the incremental design keeps request counts low.

`/metrics` carries `pond_run_seconds_total`, `pond_duck_target` and `pond_flock` so you can attribute spend per Pond.

## A checklist

Before you decide it is broken:

- [ ] `duckstring catchment settings` says `cloud: enabled`
- [ ] The Catchment role has `ecs:TagResource` and `iam:PassRole` for every worker role
- [ ] The execution role has both log-group ARN forms
- [ ] `sg-catchment` admits `7474` from `sg-duck`
- [ ] Workers have a subnet **and** security group configured
- [ ] The image architecture matches `DUCKSTRING_FARGATE_CPU_ARCH`
- [ ] (EC2) The AMI's default `python3` is 3.10+, with no zero-length files in site-packages
- [ ] The Catchment's dial-back URL has a host in it — an untokened IMDSv2 read yields an empty one, and `http://:7474` fails a minute later as an unresponsive worker
