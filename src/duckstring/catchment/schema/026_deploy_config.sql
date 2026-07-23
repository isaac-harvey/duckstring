-- UI-entered compute deployment config (plans/compute-config-ui.md). The AWS deployment settings a
-- remote Duck needs to launch — Fargate: image/task-def, subnets, security groups, execution+task roles,
-- cluster, assign_public_ip, cpu_arch; EC2: ami, instance_profile, pip_spec — were Catchment-level env
-- only. Store them as a JSON blob per pool AND per dedicated Duck, so they're UI-entered and validated,
-- coalesced over the env defaults. NULL/absent = inherit the env defaults (the hosted-platform path).
ALTER TABLE duck_pool ADD COLUMN deploy_config TEXT;  -- JSON: a named pool's provider deployment config
ALTER TABLE pond_duck ADD COLUMN deploy_config TEXT;  -- JSON: a Pond's dedicated Duck's deployment config
