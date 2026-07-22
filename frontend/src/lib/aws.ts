// AWS Fargate task-size matrix — the valid (cpu, memory) combinations (a fixed, documented table; not
// an API). A bad combo is rejected only at RunTask time, so the UI constrains it up front: pick a cpu,
// and the memory choices narrow to that cpu's allowed range. cpu is in vCPU-units (256 = 0.25 vCPU),
// memory in MiB.
//
// EC2 instance types, by contrast, ARE fetched live (DescribeInstanceTypes) — see InstanceTypePicker.

function range(lo: number, hi: number, step: number): number[] {
  const out: number[] = [];
  for (let m = lo; m <= hi; m += step) out.push(m);
  return out;
}

export const FARGATE_CPU: number[] = [256, 512, 1024, 2048, 4096, 8192, 16384];

export const FARGATE_MEMORY: Record<number, number[]> = {
  256: [512, 1024, 2048],
  512: [1024, 2048, 3072, 4096],
  1024: range(2048, 8192, 1024),
  2048: range(4096, 16384, 1024),
  4096: range(8192, 30720, 1024),
  8192: range(16384, 61440, 4096),
  16384: range(32768, 122880, 8192),
};

export function cpuLabel(cpu: number): string {
  return `${cpu} (${cpu / 1024} vCPU)`;
}

export function memoryLabel(mib: number): string {
  return `${mib} MiB (${mib / 1024} GiB)`;
}

// The default memory for a cpu (the smallest valid one) — used when a cpu is picked before a memory.
export function defaultMemoryFor(cpu: number): number | undefined {
  return FARGATE_MEMORY[cpu]?.[0];
}
