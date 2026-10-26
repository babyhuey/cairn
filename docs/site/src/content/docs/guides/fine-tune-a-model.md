---
title: Fine-tune a model on a GPU box
description: Prepare a dataset, fine-tune Qwen3-4B with QLoRA on an AWS GPU box that cairn starts and stops for you, approve the spend, and promote the result. Every command, from an empty box to a published adapter.
---

This guide runs the governed fine-tune behind
[`cairndev/qwen3-4b-ifc-bim-qlora`](https://huggingface.co/cairndev/qwen3-4b-ifc-bim-qlora),
command by command. It fine-tunes Qwen3-4B-Base with 4-bit QLoRA on an IFC/BIM
instruction dataset. The steps work the same for your own data and base model.

What you get:

- **A person approves the data source** before anything is downloaded.
- **The launch checks data roles and leakage.** It refuses a test set passed
  as training data, and training data that overlaps the eval sets.
- **A GPU cost estimate**, which a person approves before the box starts. The
  budget is capped.
- **The GPU box starts after approval and stops right after training.** It
  also stops on failure and on cancel.
- **The spend is settled per attempt** from the box's wall-clock time.
- **Promotion only after a person decides,** with the losses on the card.
- **Every step recorded.** You can read the findings and replay the run later.

Our run: 5,646 training rows, 353 steps on a Tesla T4, about 73 minutes of
training. It settled at **$0.86** against a **$16.50** worst-case estimate.
Validation loss went from **2.718** for the base model to **0.554**.

## The layout

| Machine | Runs | Installs |
|---|---|---|
| **Control box**: any small Linux machine (we used an EC2 t2.large) | the cairn engine: runs, gates, budget, the data leg, and the box's start and stop | `cairndev`, the two packs |
| **GPU box**: an EC2 GPU instance (we used a g4dn.xlarge with a 16 GB T4) | only the trainer, started over SSH by the engine | `cairn-operator-training[gpu]` |
| **Your laptop** (optional) | the `cairn remote …` CLI, through an SSH tunnel | `cairndev` |

The engine never runs on the GPU box, and nothing listens on it: the
control box reaches out over SSH. Datasets, checkpoints and the adapter cross
between the machines through one S3 bucket.

**Before you start**, you need:

1. **Two EC2 instances** in the same VPC. Give the GPU instance an Elastic
   IP, or use its private IP from the control box.
2. **SSH from the control box to the GPU box,** with an SSH key on the
   control box and port 22 open between them.
3. **An instance role on both boxes** that can read and write the S3
   bucket. On the control box, the role also needs `ec2:DescribeInstances`,
   `ec2:CreateTags`, `ec2:StartInstances` and `ec2:StopInstances` on the GPU
   instance.
4. **Public Hugging Face Hub access** for the dataset and the base model.

## 1. Install on the control box

All public wheels from PyPI. The `[all]` extra turns on near-duplicate
detection and token-length profiling:

```bash
uv venv .venv --python 3.13
uv pip install "cairndev[ssh,storage-postgres]==0.3.1" \
  "cairn-pack-dataset-prep[all]==0.1.0" "cairn-pack-finetune==0.1.1"
```

## 2. Configure and start the engine

`cairn ls finetune` prints the installed path of the pack's `finetune-ssh`
profile. Put the settings in one file. Both the engine and the CLI must read
the **same** `CAIRN_STORAGE`, or the CLI writes connections to a store the
engine never reads:

```bash
# engine.env
CAIRN_PROFILE=<path printed by `cairn ls finetune`>/finetune-ssh.toml
CAIRN_STORAGE=sqlite:///$HOME/.cairn/train.db        # or postgres://…
CAIRN_ARTIFACT_S3_BUCKET=<bucket>
CAIRN_ARTIFACT_S3_PREFIX=cairn/artifacts
CAIRN_CHECKPOINT_BUCKET=<bucket>
CAIRN_CHECKPOINT_PREFIX=cairn/checkpoints
AWS_DEFAULT_REGION=<region>
CAIRN_REMOTE_ENV=CAIRN_CHECKPOINT_BUCKET,CAIRN_CHECKPOINT_PREFIX,AWS_DEFAULT_REGION
CAIRN_DATAFLOW_REMOTE_CMD=/home/ubuntu/cairn-train/.venv/bin/cairn-dataflow-run
```

- **`CAIRN_REMOTE_ENV`** forwards these variables, by name, to the training
  job on the GPU box. The engine refuses any name containing `KEY`, `SECRET`,
  `TOKEN` or `PASSWORD`.
- **`CAIRN_DATAFLOW_REMOTE_CMD`** is the stage runner's full path on the GPU
  box. Its virtualenv is created in step 5.

Start the engine with the GPU box's SSH key in its environment. The key is
read from the control box's own file and is never stored in a connection:

```bash
set -a; . ./engine.env; set +a
export GPU_SSH_KEY="$(cat ~/.ssh/<key-to-the-gpu-box>)"
cairn dev --engine-only --port 9090 --db "$HOME/.cairn/train.db"
```

`cairn dev` is the quickest way to start. For a server you keep, run the
[`deploy/light`](/install/) stack (Postgres, sign-in) instead.

## 3. Register the GPU box

A `compute.ssh` connection named `gpu`, the name the profile's `ml_target`
uses. `instance_id` and `region` let cairn start and stop it.
`hourly_usd` prices it, which turns on the launch estimate and the budget
check:

```bash
cairn connect add ssh --name gpu \
  --set host=<gpu private ip> --set user=ubuntu \
  --set known_hosts=$HOME/.ssh/known_hosts --secret PRIVATE_KEY=GPU_SSH_KEY \
  --set instance_id=<i-…> --set region=<region> --set hourly_usd=0.66
cairn doctor --strict
```

`cairn doctor --strict` should show:

- `ml_target=gpu`, `remote_exec_mode=venv` and the forwarded `remote_env`;
- every pack's `[compat]` satisfied;
- `compute/acquire` and `compute/release` loaded.

Log the CLI in to the engine. `cairn dev` prints its development login:

```bash
cairn remote login -u dev -p dev -e http://127.0.0.1:9090
```

## 4. Prepare the dataset

`dataset-prep/prepare` pins the source, removes exact and near duplicates,
profiles lengths with the base model's tokenizer, splits by template family
so variants never straddle train and test, and registers three dataset
versions with their roles:

```bash
cairn remote presets dataset-prep/prepare
cairn remote launch dataset-prep/prepare --preset ifc-bim-alpaca
```

It pauses at **`source-ok`**. A person checks the source, its pinned commit
and the split before anything is read:

```bash
cairn remote advance <pl_id> source-ok --approver <you>
cairn remote findings <pl_id>
```

The findings are the evidence. In our run:

| check | result |
|---|---|
| source rows | 42,680 |
| exact duplicates | 0 |
| near duplicates (MinHash, 0.8) | 36,385 dropped (85%: the source is template-generated) |
| kept | 6,295 rows, 1.49 M tokens, none over 2,048 tokens |
| split | train 5,646 · val 301 · test 348, `split_no_leakage = true` |

For your own data, pass `-i path=<hf id | s3://… | file.jsonl> -i register=<name>`.
`--save-preset <name>` keeps the inputs for next time.

## 5. Install the trainer on the GPU box

Do this right before training, because the box bills while it is up. It
needs only the trainer wheel and the same contract versions as the control
box:

```bash
uv venv ~/cairn-train/.venv --python 3.13
VIRTUAL_ENV=~/cairn-train/.venv uv pip install \
  "cairn-operator-training[gpu]==0.1.0" \
  "cairn-contracts-training==1.0.0" "cairn-contracts-training-stages==1.2.0"
```

The `cairndev` package comes along as a dependency. Only
`cairn-dataflow-run` is ever called on this box. You can leave the box
stopped: `sft` starts it after you approve.

## 6. Launch the fine-tune

```bash
cairn remote launch finetune/sft --preset ifc-bim-qwen3-4b
```

Before any record exists, the launch door checks:

- each dataset version's **role** (train, val and test can't be swapped) and
  its intake verdict;
- the **holdout**: the training data must not overlap the eval sets, exactly
  or near;
- the **GPU estimate**: `hourly_usd` × each priced leg's timeout × its
  attempts, against the budget.

Ours: smoke $0.66 + train $15.84 = **$16.50** worst case, with a $25 budget.

The run pauses at **`approve-run`**, showing that estimate and the training
settings:

```bash
cairn remote advance <pl_id> approve-run --approver <you>
```

Then, in order:

1. **`acquire`** starts the box. It writes a 26-hour lease tag first, so
   `cairn compute sweep` can find an orphaned box.
2. **`smoke`** runs 5 steps on 64 rows, the whole path on the box.
3. **`train`** runs the full training. A killed process is retried once and
   resumes from its last S3 checkpoint.
4. **`release-box`** stops the box.
5. **`improved`** checks that the candidate beat the base model's eval loss.

A final `release` step stops the box on every outcome, as a backstop.

Follow it, and read the results:

```bash
cairn remote pipeline <pl_id>
cairn remote findings <pl_id>
```

Ours: `train_qlora_baseline_eval_loss 2.71788`, `train_qlora_eval_loss
0.553942`, `train_qlora_steps 353`, and the adapter at its S3 URI with a
content hash.

## 7. Decide, promote, check the spend

The run pauses at **`promote-model`** with the losses on the card. If you
promote, the `artifact/promote` operator asks for its own approval before it
moves the tag:

```bash
cairn remote advance <pl_id> promote-model --approver <you>
cairn remote advance <pl_id> promote --approver <you>
```

Each priced leg settles its real box time in `Run.cost.gpu_usd`. Ours:
smoke $0.05, train $0.81, **$0.86 in total**. Check that both boxes are
stopped:

```bash
cairn compute status gpu
```

## 8. Check it didn't forget (optional, outside cairn for now)

A lower loss on your domain says the model learned the task. It doesn't
say whether general ability dropped. Compare the base model and base +
adapter with
[lm-evaluation-harness](https://github.com/EleutherAI/lm-evaluation-harness),
using the same settings for both, on the GPU box:

```bash
uv venv ~/lmeval/.venv --python 3.13
VIRTUAL_ENV=~/lmeval/.venv uv pip install "lm-eval[hf]==0.4.13" peft bitsandbytes accelerate
BASE="pretrained=unsloth/qwen3-4b-base-unsloth-bnb-4bit,dtype=float16"
for ARGS in "$BASE" "$BASE,peft=cairndev/qwen3-4b-ifc-bim-qlora"; do
  lm_eval --model hf --model_args "$ARGS" --tasks arc_easy,arc_challenge,hellaswag,winogrande,piqa \
    --limit 150 --batch_size auto --seed 1234
  lm_eval --model hf --model_args "$ARGS" --tasks mmlu --limit 5 --batch_size auto --seed 1234
done
```

Ours took about 50 minutes on the T4. No score moved by more than the
measurement noise (about ±3–4 points at 150 samples). The tables are on the
[model card](https://huggingface.co/cairndev/qwen3-4b-ifc-bim-qlora). Stop the
box afterwards. A governed eval step inside `finetune/sft`, priced and
recorded like training, is in progress
([cairn-extensions#37](https://github.com/cairndev-sh/cairn-extensions/pull/37)).

## What to change for your own run

| | |
|---|---|
| Data | `dataset-prep/prepare -i path=… -i register=…`. Rows may be Alpaca (`instruction`/`input`/`output`), `prompt`/`completion`, or chat `messages`. |
| Base model | `-i base_model=<any Hugging Face causal model>`. The GPU's memory limits the size: a 16 GB T4 fits about 4B with QLoRA. |
| Recipe | `method`, `lora_r`, `lora_alpha`, `learning_rate`, `max_seq_len`, `epochs`, `per_device_batch_size`, `gradient_accumulation_steps` |
| Your own box | `finetune/sft-own-box` is the same pipeline without the start and stop steps, for a box you manage. |
| Tracking | Optional: `-i report_to=mlflow` with `MLFLOW_TRACKING_URI` set on the control box. |

## Known limits

- **Supported scope:** one GPU box per leg, supervised fine-tuning only, text
  only.
- **Same box, unchecked:** cairn does not yet check that `compute_connection`
  matches the profile's `ml_target`
  ([cairn#216](https://github.com/cairndev-sh/cairn/issues/216)). Keep both
  `gpu`.
- **No audit trail by default:** the `finetune-ssh` profile does not bind an
  audit sink yet
  ([cairn-extensions#38](https://github.com/cairndev-sh/cairn-extensions/issues/38)).
  Approvals and run events are still recorded in the run store.
- **Token expiry:** the CLI's login expires. Run `cairn remote login` again if
  a command answers `401`.
