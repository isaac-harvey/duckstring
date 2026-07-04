import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';

import styles from './index.module.css';

// The landing page (duckstring.com). Doubles as the canonical "what is this" intro — long enough
// that posting the bare link reads like the top of a launch post. It is styled on the product UI's
// dark canvas regardless of the site colour mode — the palette follows frontend/src/lib/store.ts.
//
// POSITIONING (settled — keep this order):
//   1. Brand statement + one-line "what is this". pip install is the primary conversion — weight it.
//   2. WhatIsThis: the one decision (transforms as packages) and its three payoffs — the intro
//      bullets anchor-link down to the section that demonstrates each.
//   3. The payoff sections, in bullet order: atomic concurrent-version upgrade (#upgrade), the
//      bidirectional throttle (#demand — the moat/wow), then incremental as the reveal (#incremental).
//   4. The lightweight on-ramp — wrap work you already run (#start).
// Demos are show-don't-tell: each DemoSlot is a placeholder for a video the author drops in.
// Never call it an "orchestration framework"; never name competitors; don't lead with the Catchment.

// ─────────────────────────────────────────────────────────────────────────────
interface DemoSlotProps {
  src: string;        // Path to your video (e.g., '/img/demo-analytics') without extension
  aspectRatio?: string; // Optional: Allows custom aspect ratios like '16/9', '4/3', '1/1'
  poster?: string;     // Optional: Path to a static fallback screenshot
}

function DemoSlot({ 
  src, 
  aspectRatio = '16/9', 
  poster 
}: DemoSlotProps): ReactNode {
  return (
    <figure className={styles.demo}>
      <div 
        className={styles.demoFrame} 
        style={{ aspectRatio }} // Inline style handles dynamic aspect ratios natively
      >
        <video 
          className={styles.demoMedia} 
          autoPlay 
          loop 
          muted 
          playsInline 
          poster={poster}
        >
          {/* Delivers WebM first for optimized browsers, falls back to MP4 */}
          <source src={useBaseUrl(`${src}.webm`)} type="video/webm" />
          <source src={useBaseUrl(`${src}.mp4`)} type="video/mp4" />
          Your browser does not support the video tag.
        </video>

        <span className={styles.demoPlay} aria-hidden>
          ▶
        </span>
      </div>
    </figure>
  );
}

function Section({
  id,
  kicker,
  title,
  children,
  alt,
}: {
  id?: string;
  kicker?: string;
  title: string;
  children: ReactNode;
  alt?: boolean;
}): ReactNode {
  return (
    <section id={id} className={alt ? styles.sectionAlt : styles.section}>
      <div className={styles.sectionInner}>
        {kicker && <p className={styles.kicker}>{kicker}</p>}
        <h2 className={styles.sectionTitle}>{title}</h2>
        {children}
      </div>
    </section>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

function Hero(): ReactNode {
  return (
    <header className={styles.hero}>
      <img src={useBaseUrl('/img/logo-mark.svg')} alt="" className={styles.mark} />
      <p className={styles.wordmark}>Duckstring</p>
      <h1 className={styles.tagline}>There is no DAG.</h1>
      <div className={styles.install}>
        <span className={styles.installPrompt} aria-hidden>
          $
        </span>
        <code className={styles.installCmd}>pip install duckstring</code>
      </div>
      <p className={styles.lead}>
        Build data pipelines the way you build software.
        Version your transforms, declare your dependencies, and let Duckstring
        execute only the paths your data demands.
      </p>
      <div className={styles.ctaRow}>
        <Link className={styles.ctaPrimary} to="/getting-started/quickstart">
          Quickstart →
        </Link>
        <Link className={styles.ctaGhost} href="https://playground.duckstring.com">
          Try the playground
        </Link>
      </div>
      <p className={styles.heroNote}>Apache-2.0 · pure Python · no service to stand up</p>
    </header>
  );
}

// The 30-second "what is this", in prose. One decision, three payoffs — each bullet anchors down to
// the section that demonstrates it.
function WhatIsThis(): ReactNode {
  return (
    <Section kicker="Declare, don't govern" title="Know your family, not the world.">
      <p className={styles.prose}>
        Duckstring operates on a core decision:{' '}
        <strong>treat each transform as a versioned package</strong> (a Pond) that declares its
        upstream dependencies, exactly the way a library declares the packages it imports.
        You need only work with your immediate sources and consumers — not the entire lineage.
        Make that one decision and you get three things that are normally hand-built and hand-tended for free:
      </p>
      <ul className={styles.payoffs}>
        <li>
          <a className={styles.payoffLink} href="#upgrade">
            <strong>DAG is implied — upgrade atomically.</strong>
            <span className={styles.payoffArrow}> ↓</span>
          </a>{' '}
          The pipeline is the union of every Pond&apos;s declared dependencies. There&apos;s no
          central DAG to build, wire, or govern — it&apos;s already in the graph. A new Pond (or a breaking change to an existing one)
          won&apos;t execute until there&apos;s a consumer ready to use it.
        </li>
        <li>
          <a className={styles.payoffLink} href="#demand">
            <strong>Demand-driven execution.</strong>
            <span className={styles.payoffArrow}> ↓</span>
          </a>{' '}
          Runs are driven from the <em>outputs</em>, not the inputs — paths with no downstream
          consumers sit idle, and each path runs only as often as its bottleneck, throttled both
          downstream <em>and upstream</em>.
        </li>
        <li>
          <a className={styles.payoffLink} href="#incremental">
            <strong>Native incremental processing.</strong>
            <span className={styles.payoffArrow}> ↓</span>
          </a>{' '}
          Run history is metadata, which makes change detection and incremental processing trivial.
          Duckstring bundles Trickle: a DBSP-based incremental engine over DuckDB. Blazing-fast
          execution on a single node — perfect for the 90% of cases where you don&apos;t{' '}
          <em>actually</em> need distributed compute.
        </li>
      </ul>
      <p className={styles.proseMuted}>
        <a className={styles.payoffLink} href="#start">
          <strong>Duckstring is generic</strong>
          <span className={styles.payoffArrow}> ↓</span>
        </a>{' '}
         — attach any Python code (even calls out to external services) and get
        the full benefit immediately.
      </p>
    </Section>
  );
}

// THE HERO DEMO — the bidirectional throttle. The most unique behaviour, and the one no
// schedule-driven tool can reproduce.
function ThrottleDemo(): ReactNode {
  return (
    <Section
      id="demand"
      kicker="Run only what's demanded"
      title="Bottleneck-aware execution. No wasted compute."
      alt>
      <p className={styles.prose}>
        Most schedulers can throttle work <em>downstream</em> of a slow step. Duckstring throttles
        everything <em>upstream</em> of it too. Execution is strictly demand-driven: a transform runs
        only when something downstream has actually asked for it. The result is a pipeline that
        re-paces itself to its real bottleneck — and never over-produces results no one is waiting
        for.
      </p>

      <DemoSlot src="/img/ripple" aspectRatio="28/10"/>

      <p className={styles.proseMuted}>
        No sophisticated prediction of run times is required — flipping to control by consumers rather
        than suppliers means the entire path naturally throttles to its slowest process. See{' '}
        <Link to="/theory">Orchestration Theory</Link>.
      </p>
    </Section>
  );
}

// THE CLOSER DEMO — seamless upgrade. The thing that's impossible today and hits the coordination
// pain squarely.
function UpgradeDemo(): ReactNode {
  return (
    <Section id="upgrade" kicker="Upgrade atomically" title="Ship a breaking change without a meeting." alt>
      <p className={styles.prose}>
        Ponds use SemVer, and a new major version runs <strong>concurrently</strong> with the old
        one. Deploy a breaking <code>v2</code> and it comes up <em>alongside</em> <code>v1</code>:
        existing consumers keep pulling <code>v1</code>, which keeps running, while consumers migrate
        to <code>v2</code> one at a time by changing a single line in their own manifest. The old
        major retires when nothing depends on it. No lockstep, no choreographed release, no freeze.
      </p>

      <DemoSlot src="/img/upgrade" aspectRatio="28/10"/>

      <p className={styles.proseMuted}>
        Upgrading a complex sequence of transformations can paralyze development, especially if
        upstream changes are needed. Just deploy breaks as a separate Pond, know that it won&apos;t
        run until downstream also upgrades, and be sure you won&apos;t have broken anything. See{' '}
        <Link to="/concepts/versioning">Versioning</Link>.
      </p>
    </Section>
  );
}

// THE REVEAL — incremental compute. Framed as a consequence of the package boundary.
function IncrementalReveal(): ReactNode {
  return (
    <Section
      id="incremental"
      kicker="Keep tasks small with deltas"
      title="Discrete stages for incremental processing.">
      <p className={styles.prose}>
        Incremental processing becomes very natural once a Pond has a clear lineage, runs only when
        parents change, and tracks an epoch throughout. Bundled with Duckstring is the Trickle engine
        — a DBSP implementation over DuckDB that cuts processing to the absolute minimum by focussing
        only on changes.
      </p>

      <DemoSlot src="/img/trickle" aspectRatio="28/10"/>

      <p className={styles.proseMuted}>
        Done well, incremental processing lets you stay single-node, in-memory and blazing fast —
        real streaming performance with minimal infrastructure. See{' '}
        <Link to="/guides/trickle">Incremental processing</Link>.
      </p>
    </Section>
  );
}

// THE ON-RAMP — the lightweight immediate win. A Ripple is generic code, so a Pond can wrap work you
// already run on other systems and simply stop re-running the parts that don't need it.
function OnRamp(): ReactNode {
  return (
    <Section id="start" kicker="Start small" title="Drop it into the stack you already run." alt>
      <p className={styles.prose}>
        You don&apos;t have to move your compute to get value. A Pond is just Python, so it can wrap
        anything: a SQL transform, a local script, or a call out to a remote system that it kicks off
        and polls to completion. Point Duckstring at a sequence you already run and it becomes the
        coordinator — firing each step only when its inputs have actually changed and something
        downstream wants the result. The redundant compute you stop paying for lands on day one, with
        no rewrite.
      </p>
      <div className={styles.useCases}>
        <div className={styles.useCase}>
          <span className={styles.useCaseTitle}>Coordinate, don&apos;t migrate</span>
          <span className={styles.useCaseBody}>
            Wrap remote jobs behind a start-and-poll code chunk; Duckstring sequences them by demand,
            skipping the runs whose inputs are unchanged.
          </span>
        </div>
        <div className={styles.useCase}>
          <span className={styles.useCaseTitle}>Cut wasted runs</span>
          <span className={styles.useCaseBody}>
            No part of the pipeline runs unless a consumer actually needs it. If
            nothing upstream moved, nothing runs — the compute saving lands on day one.
          </span>
        </div>
        <div className={styles.useCase}>
          <span className={styles.useCaseTitle}>Grow into the model</span>
          <span className={styles.useCaseBody}>
            Promote a wrapped step to a native transform when it earns it; start with API
            calls to existing systems and shift it to Duckstring whenever you feel.
          </span>
        </div>
      </div>
    </Section>
  );
}

const ROUTES: {title: string; body: string; to?: string; href?: string}[] = [
  {
    title: 'Quickstart',
    body: 'A running four-Pond pipeline in a few minutes — install, deploy, trigger, query.',
    to: '/getting-started/quickstart',
  },
  {
    title: 'Playground',
    body: 'Feel freshness-based execution in your browser — build a graph, send demand, nothing to install.',
    href: 'https://playground.duckstring.com',
  },
  {
    title: 'Theory',
    body: 'The full demand-and-freshness model, worked step by step. The part to read if the demo intrigued you.',
    to: '/theory',
  },
  {
    title: 'GitHub',
    body: 'The source: the packaging standard, the CLI, and the reference runtime. Apache-2.0.',
    href: 'https://github.com/duckstring-dev/duckstring',
  },
];

function Routes(): ReactNode {
  return (
    <Section kicker="Go deeper" title="Where to next.">
      <div className={styles.routes}>
        {ROUTES.map((r) => (
          <Link key={r.title} className={styles.routeCard} to={r.to} href={r.href}>
            <span className={styles.routeTitle}>
              {r.title} <span className={styles.routeArrow}>→</span>
            </span>
            <span className={styles.routeBody}>{r.body}</span>
          </Link>
        ))}
      </div>
    </Section>
  );
}

function Hosting(): ReactNode {
  return (
    <section className={styles.hosting}>
      <p>
        Looking for cloud hosting — a Catchment run for you? I&apos;d like to hear from you:{' '}
        <a href="mailto:dev@duckstring.com">dev@duckstring.com</a>
      </p>
    </section>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      description="Build data pipelines the way you build software: version each transform, declare its dependencies, and Duckstring resolves and runs the execution DAG on demand — no schedules, no governance.">
      <main className={styles.canvas}>
        <Hero />
        <WhatIsThis />
        <UpgradeDemo />
        <ThrottleDemo />
        <IncrementalReveal />
        <OnRamp />
        <Routes />
        <Hosting />
      </main>
    </Layout>
  );
}
