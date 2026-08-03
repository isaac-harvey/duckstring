-- A Duck Pool picks its remote compute PROVIDER (plans/cloud-config.md §4b). Fargate is the default
-- (fast serverless containers); EC2 is the escape hatch for big/GPU/warm-pool jobs. A Fargate pool
-- carries only its size (cpu/memory — the Fargate task size); an EC2 pool keeps instance_type. The
-- Fargate INFRA (cluster/subnets/roles/image) is Catchment-level env, not per-pool.
ALTER TABLE duck_pool ADD COLUMN provider TEXT;   -- 'fargate' (default when NULL) | 'ec2'
ALTER TABLE duck_pool ADD COLUMN cpu      INTEGER; -- Fargate task cpu units (256 = 0.25 vCPU, 1024 = 1 vCPU)
ALTER TABLE duck_pool ADD COLUMN memory   INTEGER; -- Fargate task memory, MiB
