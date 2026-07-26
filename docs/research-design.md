# Research Design

## Question

Do common quality metrics predict deployment viability for image-to-3D scene generation models?

## Scope

- single-image-conditioned generation
- RGB, depth-augmented, and panorama inputs
- generator outputs evaluated by separate evaluator runners

## Metric Families

Operational:

- latency
- failure rate
- retry count
- memory use
- artifact size
- estimated cost

Quality:

- image/render similarity
- multi-view consistency
- geometry quality
- perceptual render quality
- optional human ratings

Decision:

- weighted utility by use case
- latency-constrained quality
- cost-adjusted quality

## Controls

- same sample cohorts across models
- fixed hardware per campaign
- frozen runner images and config per campaign
- failures recorded explicitly
- generation and evaluation separated

## Analysis

- descriptive statistics
- ranking comparison across metric families
- quality/deployment correlation
- sensitivity over use-case weights
- pairwise tests when sample size permits
