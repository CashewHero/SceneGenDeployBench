# Pipelines

A pipeline connects DeployBench runners, script containers, and other pipelines. Runner stages create ordinary durable jobs. Script stages execute directly with the same container launcher as `deploybench run`. Child-pipeline stages create durable pipeline runs.

Catalog definitions live in `config/pipelines/*.yaml`.

## Pipeline definition

```yaml
pipeline_version: 1
name: image_quality
dataset: example_set1/city
runner: image_generator

parameters:
  max_references: 20

matrix:
  seed: [0, 1]

stages:
  generate:
    runner: ${{ runner }}
    inputs:
      data: ${{ dataset }}
    with:
      seed: ${{ matrix.seed }}

  evaluate:
    needs: generate
    runner: image_evaluator
    inputs:
      data: ${{ dataset }}
      candidate: ${{ stages.generate.outputs }}
      references: ${{ dataset }}
    with:
      max_references: ${{ parameters.max_references }}
```

The top-level fields are:

| Field | Meaning |
| --- | --- |
| `pipeline_version` | Definition format version; currently `1` |
| `name` | Pipeline catalog name |
| `dataset` | Default dataset target |
| `runner` | Default runner selector |
| `parameters` | Named author-defined values |
| `matrix` | Axes used to create execution lanes |
| `stages` | Dependency graph of work |

`dataset` and `runner` are pipeline input defaults. A pipeline invocation can replace them with `--dataset` and `--runner`. An effective dataset value is required when the pipeline is added; `runner` is required only when a stage uses `${{ runner }}`.

### Parameters

Parameters hold reusable pipeline values:

```yaml
parameters:
  max_references: 20
  settings:
    save_renders: false
```

Override a declared top-level parameter with repeatable `--set` options:

```bash
deploybench pipeline add image_quality \
  --set max_references=40
```

Use parameter values with `${{ parameters.<path> }}`:

```yaml
with:
  max_references: ${{ parameters.max_references }}
  save_renders: ${{ parameters.settings.save_renders }}
```

### Matrix

Each matrix key is an axis containing a nonempty list. The pipeline creates the Cartesian product of all axes:

```yaml
matrix:
  sigma_px: [0, 1, 2]
  seed: [0, 1]
```

This matrix creates six lanes. A matrix-scoped stage runs once in each lane and reads the current values with `${{ matrix.<path> }}`.

Override declared axes when adding a pipeline:

```bash
deploybench pipeline add image_quality \
  --matrix sigma_px=0,2 \
  --matrix seed=0,1
```

### Replacers

A replacer must occupy the complete YAML value. It preserves the selected JSON type, so a number remains a number, a list remains a list, and a mapping remains a mapping. Replacers are not interpolated inside larger strings.

| Replacer | Selected value |
| --- | --- |
| `${{ dataset }}` | Pipeline dataset target string |
| `${{ runner }}` | Pipeline runner selector string |
| `${{ parameters.<path> }}` | Value under `parameters` |
| `${{ matrix.<path> }}` | Value from the current matrix lane |
| `${{ stages.<stage>.outputs }}` | Complete result from a dependency stage |
| `${{ stages.<stage>.outputs.<path> }}` | Nested value from a dependency result |

For example, if `discover` stage returns:

```json
{
  "runner": "image_evaluator@0.1.0",
  "settings": {
    "max_references": 40
  }
}
```

a dependent stage can select either field:

```yaml
evaluate:
  needs: discover
  runner: ${{ stages.discover.outputs.runner }}
  with:
    max_references: ${{ stages.discover.outputs.settings.max_references }}
```

A stage referenced by `${{ stages.<stage>.outputs... }}` must be listed in `needs`. One applicable dependency execution returns one selected value. Several applicable executions return a list containing one value from each execution.

Replacers work in stage value fields such as selectors, `inputs`, `with`, `image`, `run`, `env`, `access`, `mounts`, `workdir`, and child-pipeline overrides. Scheduling fields are static because the orchestrator needs them before execution.

## Stages

Every stage uses exactly one type:

- A runner stage defines `runner`.
- A script stage defines `image` and `run`.
- A child-pipeline stage defines `pipeline`.

All stage types share these scheduling fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `needs` | `[]` | One dependency stage id or a list of ids |
| `if` | `success()` | `success()` skips after a failed dependency; `always()` still runs |
| `scope` | `matrix` | `matrix` runs per lane; `pipeline` runs once for the complete pipeline |
| `timeout-minutes` | `60` | Maximum stage execution time |

Runner and script stages also accept `retention`:

| Value | Behavior |
| --- | --- |
| `keep` | Keep outputs after the pipeline; this is the default |
| `pipeline` | Delete outputs when the pipeline finishes |
| `matrix` | Delete outputs when the matrix lane finishes |
| `none` | Delete outputs as soon as the stage execution finishes |

On a pipeline-scoped stage, `retention: matrix` behaves as `pipeline`. A later runner input cannot consume files from a stage using `retention: none`. Child-pipeline stages do not accept `retention`; configure retention inside the child pipeline.

A matrix-scoped stage depending on a pipeline-scoped stage uses the same dependency result in every lane. A pipeline-scoped stage depending on a matrix-scoped stage waits for every lane and sees all applicable dependency executions.

### Runner stage

A runner stage selects a registered runner, maps pipeline sources into runner input roles, and supplies job parameters through `with`:

```yaml
generate:
  runner: ${{ runner }}
  inputs:
    data: ${{ dataset }}
  with:
    seed: ${{ matrix.seed }}
```

`inputs` supports the `data`, `candidate`, and `references` roles. Required roles depend on the selected runner's input contract. Dataset-downloader runners may omit `inputs`.

Runner inputs accept indexed targets or produced files:

| Input value | Behavior |
| --- | --- |
| A target string such as `${{ dataset }}` | Index that dataset or output target |
| `${{ stages.select.outputs.target }}` | Resolve the field to a target string or list, then index those targets |
| `${{ stages.generate.outputs }}` | Consume only `output_files` produced by the dependency |

These forms are intentionally different. Suppose a selection stage returns an indexed target:

```json
{
  "target": "example_set1/city/frame_000010"
}
```

Select the target field explicitly:

```yaml
inputs:
  data: ${{ stages.select.outputs.target }}
```

If a stage produced new files, use its bare outputs:

```yaml
inputs:
  candidate: ${{ stages.generate.outputs }}
```

In this runner-input position, bare outputs ignore ordinary result fields and use only `output_files` from applicable completed executions. There is no target-field fallback.

Each runner-backed stage execution links to an ordinary DeployBench job. Its job result becomes the stage result used by later `${{ stages.<stage>.outputs... }}` replacers.

### Script stage

A script stage runs a container directly:

```yaml
discover:
  image: python:3.12-slim
  run: python -c 'print("discover")'
  env:
    DATASET_TARGET: ${{ dataset }}
  access: [datasets]
  mounts: []
  workdir: /workspace
```

`run` may be a shell command string or an exact argument list. `access`, `env`, `mounts`, and `workdir` have the same meanings as in `deploybench run`.

The script receives `/workspace/pipeline.json` with pipeline information, the current stage and matrix lane, and dependency results:

```json
{
  "pipeline": {
    "run_id": "...",
    "name": "image_quality",
    "dataset": "example_set1/city",
    "runner": "image_generator@0.1.0",
    "parameters": {"max_references": 20}
  },
  "stage": {
    "id": "discover",
    "scope": "pipeline",
    "lane_index": null,
    "matrix": {}
  },
  "needs": {}
}
```

The script may return a JSON object by writing to the path in `$DEPLOYBENCH_RESULT`. Every ordinary field is available through `${{ stages.<stage>.outputs.<path> }}`:

```json
{
  "target": "example_set1/city/frame_000010",
  "settings": {
    "max_references": 40
  }
}
```

To expose newly produced files to a later runner stage, return the reserved `output_files` mapping:

```json
{
  "output_files": {
    "summary": {
      "text": "report.txt"
    }
  }
}
```

`output_files` has the shape `sample_id -> data_type -> path`. Paths are relative to `/workspace`; stored result paths are rewritten to their persistent locations.

A script may return no result. New files created under `/workspace` are still copied into its execution directory; `pipeline.json`, `result.json`, and orchestrator-provided files are excluded. Pipeline-scoped scripts and single-lane matrix scripts use `/data/pipelines/<pipeline>/<timestamp>/<stage>/`. Matrix-scoped scripts with several lanes add `/<lane>/`.

### Child-pipeline stage

A child-pipeline stage creates one durable child pipeline run:

```yaml
calibrate:
  needs: discover
  pipeline: trajectory_calibration
  dataset: ${{ stages.discover.outputs.target }}
  runner: ${{ runner }}
  with:
    max_references: ${{ parameters.max_references }}
  matrix:
    seed: [0, 1]
```

The fields override the child definition:

| Field | Meaning |
| --- | --- |
| `pipeline` | Child pipeline catalog name |
| `dataset` | Child dataset input |
| `runner` | Child runner input |
| `with` | Child parameter overrides |
| `matrix` | Child matrix overrides |

`with` may contain only parameters declared by the child. A matrix mapping may contain only axes declared by the child.

A complete matrix replacer replaces every child matrix axis:

```yaml
matrix: ${{ stages.discover.outputs.matrix }}
```

The selected value must be a nonempty mapping of declared axes to nonempty lists. A mapping can instead replace selected axes while keeping the child's other defaults:

```yaml
matrix:
  trajectory: ${{ stages.discover.outputs.matrix.trajectory }}
```

Here `trajectory` must resolve to a nonempty list. The dynamic lanes are expanded inside one child pipeline run; they do not create one child pipeline per discovered entry.

## Running pipelines

Add a catalog pipeline by name or another definition by file path:

```bash
deploybench pipeline add image_quality \
  --dataset example_set1/city \
  --runner image_generator \
  --set max_references=40 \
  --matrix seed=0,1

deploybench pipeline add --file /path/to/pipeline.yaml \
  --dataset example_set1/city
```

See [CLI Pipelines](cli.md#pipelines) for all commands and options.

The scheduler starts ready stages during its normal poll. Pipeline state, child pipeline runs, and runner jobs survive orchestrator restarts. Cancelling a pipeline cancels unfinished jobs and active script containers.
