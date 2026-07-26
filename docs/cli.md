# CLI

`deploybench` manages config, datasets, runners, jobs, batches, and indexed outputs.

## Global Flags

- `--config <path>`
- `--json`
- `--format table|text|json`
- `--quiet`
- `--verbose`

## Commands

```bash
deploybench serve [--host <host>] [--port <port>]
deploybench run [<script>] --image <image> [options] [-- <command-or-arguments>]
deploybench config show|validate|sources
deploybench runner list|show|status
deploybench dataset list|show|download|rescan
deploybench job add|list|show|update|cancel
deploybench batch list|show
deploybench output list|show
deploybench pipeline list|validate|add|runs|show|cancel
```

## Service

```bash
deploybench serve --host 0.0.0.0
```

Runs the HTTP service and scheduler loop.

## Run

Run a script in a temporary container:

```bash
deploybench run ./analysis.py --image analysis:latest \
  --access datasets,output,database
```

Arguments after `--` are passed to a script:

```bash
deploybench run ./analysis.sh --image alpine:latest -- first second
```

With no script, arguments after `--` are the command:

```bash
deploybench run --image alpine:latest -- sh -lc 'ls /data/datasets'
```

Every invocation gets an ephemeral writable `/workspace`. System access is opt-in with repeatable or comma-separated `--access` values:

- `datasets`: `/data/datasets`, read-only
- `output`: `/data/output`, read-only
- `pipelines`: `/data/pipelines`, read-write
- `model-cache`: `/data/model_cache`, read-write
- `database`: configured PostgreSQL credentials and network
- `all`: all of the above

Use repeated `--env KEY=VALUE` and `--mount SOURCE:TARGET[:ro|rw]` for additional inputs. Mount sources must be visible inside the orchestrator.

## Config

```bash
deploybench config show
deploybench config validate
deploybench config sources
```

## Runners

```bash
deploybench runner list
deploybench runner show test_runner
deploybench runner status test_runner
```

Runner selectors can be:

- `name`
- `name@version`
- `name@latest`

## Datasets

```bash
deploybench dataset list
deploybench dataset list testset1/subset
deploybench dataset show testset1 --sample
deploybench dataset download tartanair-pano --runner tartanair --set mode=pano --set modality=image --set env=AbandonedFactory2
deploybench dataset rescan tartanair-pano
```

Dataset targets can be dataset names, subset paths, or sample paths.

Run `dataset rescan <dataset>` after a downloader or another process changes that dataset, or omit the dataset name to rebuild the complete index.

## Jobs

Create jobs:

```bash
deploybench job add --dataset testset1 --runner test_runner
deploybench job add --dataset testset1/subset --runner test_runner
deploybench job add --candidate output/my_generator/testset1 --runner my_evaluator --set metrics=psnr,ssim
deploybench job add --candidate output/my_generator/testset1/sample-1 --runner my_evaluator --reference testset1/references
```

Runner catalogs may define default `job_parameters`. Repeated `--set key=value` options override those defaults for the jobs being created. Override values are converted to the type of the catalog default.

`--dataset`, `--candidate`, and `--reference` accept either dataset or output targets and map directly to their input roles. With only `--candidate`, the candidate's original job's `inputs.data` is reused; an explicit `--dataset` replaces that default.

List and inspect:

```bash
deploybench job list
deploybench job list --dataset testset1 --runner test_runner --state pending
deploybench job list --view jobs --failed
deploybench job show <job-id>
```

Update scheduling override:

```bash
deploybench job update --job <job-id> --allow-outside-window
deploybench job update --job <job-id> --disallow-outside-window
```

Cancel pending jobs:

```bash
deploybench job cancel --job <job-id>
deploybench job cancel --dataset testset1/subset
```

Important `job add` options:

- `--dataset <dataset-or-output-path>`
- `--candidate <dataset-or-output-path>`
- `--reference <dataset-or-output-path>`; repeat as needed
- `--runner <name-or-selector>`
- `--timeout-minutes <n>` overrides the runner's `scheduling.job_timeout_minutes` default
- `--source-job <job-id>`
- `--allow-outside-window`

## Batches

```bash
deploybench batch list
deploybench batch list --runner test_runner --open
deploybench batch show <batch-id>
```

## Outputs

```bash
deploybench output list
deploybench output list --runner my_generator --dataset testset1
deploybench output show output/my_generator/testset1
```

Generated outputs are indexed from completed jobs that return a non-empty `output_files` mapping.

## Filters And Limits

Common list options:

- `--limit <n>`
- `--all`
- `--created-since <duration-or-timestamp>`
- `--updated-since <duration-or-timestamp>`
- `--finished-since <duration-or-timestamp>`

Durations: `30m`, `2h`, `7d`, `1w`.

## Pipelines

```bash
deploybench pipeline list
deploybench pipeline validate <name>
deploybench pipeline validate --file <path>
deploybench pipeline add <name> --dataset <target>
deploybench pipeline add --file <path> --dataset <target>
deploybench pipeline runs
deploybench pipeline show <pipeline-run-id>
deploybench pipeline cancel <pipeline-run-id>
```

Use repeatable `--matrix key=value1,value2` options to override axes declared by the pipeline. `--allow-outside-window` is applied to runner jobs created by the pipeline.
