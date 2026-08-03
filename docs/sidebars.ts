import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'index',
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting-started/playground',
        'getting-started/installation',
        'getting-started/quickstart',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      items: [
        'concepts/ponds',
        'concepts/ripples',
        'concepts/trickle',
        'concepts/puddles',
        'concepts/catchment',
        'concepts/freshness',
        'concepts/versioning',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/running-a-catchment',
        'guides/cloud',
        'guides/creating-a-pond',
        'guides/local-testing',
        'guides/deploying',
        'guides/triggers',
        'guides/windows',
        'guides/control',
        'guides/fault-tolerance',
        'guides/trickle',
        'guides/incremental-ripples',
        'guides/external-pipelines',
        'guides/dbt',
        'guides/connecting-catchments',
        'guides/querying-data',
        'guides/lineage',
        'guides/web-ui',
      ],
    },
    'theory',
    'incremental-theory',
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/cli',
        'reference/python-api',
        'reference/pond-toml',
        'reference/http-api',
        'reference/architecture',
      ],
    },
  ],
};

export default sidebars;
