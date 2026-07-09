# Real Data Testing

Duckstring has not yet been tested on real data streams. These should be included as demos to show real performance on recognisable datasets.

Two data sources make the most sense:
- TPC-DS
- GHArchive

These are ideal given their data size in the hundreds of megabytes to gigabytes, with relatively small deltas. They can showcase a fairly complex series of joins and aggregations.

I would like you to make a demo consisting of at least four Ponds for each of these two cases, including joins and aggregations, potentially resulting in multple Outlets, and potentially having entirely separated paths. This is to demo running one path at a different frequency to another. The Inlets should be configured such that either real new data is streamed in as deltas (e.g. for GHArchive), or such streaming is at least emulated (e.g. for TPC-DS). The Ponds should use Trickle.

These demos should then be added to the demo set, with the options --tpcds and --gharchive.

If the auth or execution/generation for these makes things too difficult/awkward to include as demos, don't try - instead write to .sandbox/tpcds and .sandbox/gharchive.

Please rewrite this document as a true plan, then confirm with me whether to continue.