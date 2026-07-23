'use client';

import type { ComputeDefaults } from '@/lib/api';

// The AWS deployment fields a remote Duck needs (plans/compute-config-ui.md), shared by the Cloud-menu
// pool form and a Pond's Dedicated Duck. Fields already supplied by the Catchment env show a muted "env:…"
// placeholder and are optional; the rest are required to launch. `deployConfigMissing` mirrors the backend
// validation so the caller can gate its save button.

type Provider = 'fargate' | 'ec2';
type Cfg = Record<string, string>;

const FIELDS: Record<Provider, { key: string; label: string; ph: string; required?: boolean }[]> = {
  fargate: [
    { key: 'image', label: 'Container image', ph: '…dkr.ecr….amazonaws.com/duckstring:tag', required: true },
    { key: 'task_definition', label: 'or task-def ARN', ph: 'family:revision (skips image registration)' },
    { key: 'subnets', label: 'Subnets', ph: 'subnet-aaa,subnet-bbb', required: true },
    { key: 'security_groups', label: 'Security groups', ph: 'sg-aaa,sg-bbb' },
    { key: 'execution_role', label: 'Execution role', ph: 'arn:aws:iam::…:role/exec (image pull + logs)', required: true },
    { key: 'task_role', label: 'Task role', ph: 'arn:aws:iam::…:role/task (S3 access)', required: true },
    { key: 'cluster', label: 'Cluster', ph: 'default' },
    { key: 'assign_public_ip', label: 'Public IP', ph: 'ENABLED' },
    { key: 'cpu_arch', label: 'CPU arch', ph: 'X86_64' },
  ],
  ec2: [
    { key: 'ami', label: 'AMI', ph: 'ami-… (with duckstring installed)', required: true },
    { key: 'instance_profile', label: 'Instance profile', ph: 'duckstring-worker (IAM for S3)', required: true },
    { key: 'pip_spec', label: 'pip spec', ph: 'duckstring==x.y (boot override)' },
  ],
};

// The required deployment fields still unsatisfied by input OR env — empty means ready to launch.
export function deployConfigMissing(provider: Provider, value: Cfg, defaults: ComputeDefaults | null): string[] {
  const env = defaults?.[provider]?.env ?? {};
  const has = (k: string) => (value[k]?.trim() || env[k]) ? true : false;
  if (provider === 'ec2') return ['ami', 'instance_profile'].filter((k) => !has(k));
  const miss = ['subnets', 'execution_role', 'task_role'].filter((k) => !has(k));
  if (!has('image') && !has('task_definition')) miss.unshift('image');
  return miss;
}

export function DeployConfigForm({ provider, value, onChange, defaults }: {
  provider: Provider;
  value: Cfg;
  onChange: (v: Cfg) => void;
  defaults: ComputeDefaults | null;
}) {
  const env = defaults?.[provider]?.env ?? {};
  const set = (k: string, v: string) => onChange({ ...value, [k]: v });
  const missing = deployConfigMissing(provider, value, defaults);
  const input: React.CSSProperties = {
    width: '100%', boxSizing: 'border-box', background: '#1a1a1f', border: '1px solid #3f3f46',
    borderRadius: 4, color: '#e4e4e7', padding: '3px 6px', fontSize: 11, fontFamily: 'inherit',
  };
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {FIELDS[provider].map((f) => {
        const fromEnv = env[f.key] != null;
        return (
          <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <span style={{ fontSize: 10, color: '#a1a1aa' }}>
              {f.label}{f.required && <span style={{ color: '#f59e0b' }}> *</span>}
              {fromEnv && <span style={{ color: '#52525b' }}> · from env</span>}
            </span>
            <input
              value={value[f.key] ?? ''}
              onChange={(e) => set(f.key, e.target.value)}
              placeholder={fromEnv ? `env: ${env[f.key]}` : f.ph}
              style={input}
              autoComplete="off"
            />
          </div>
        );
      })}
      {missing.length > 0 && (
        <div style={{ fontSize: 10, color: '#ee9333', lineHeight: 1.4 }}>
          Required to launch: {missing.join(', ')} (set here, or the DUCKSTRING_{provider.toUpperCase()}_* env)
        </div>
      )}
    </div>
  );
}
