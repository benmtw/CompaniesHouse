# Companies House Rate-Limit Probe (25% Pace)

## Purpose

Re-test the trusts that failed with Companies House `429` in random run `5`, using paced request scheduling at **25% of documented max rate**.

- Documented max: `600 requests / 5 minutes` (`2 requests/second`)
- Probe pacing: `0.5 requests/second` (one CH request every `2.0` seconds)

## Trusts Re-tested

- `09610951` (MAYFLOWER SPECIALIST SCHOOL ACADEMY TRUST)
- `09174628` (THE STOUR FEDERATION)
- `08164889` (KING JAMES'S SCHOOL)
- `08160034` (ST. MICHAEL'S CATHOLIC COLLEGE)

## Probe Details

- Run type: `companies_house_probe_25pct_rate`
- Timestamp (UTC): `2026-02-18T16:56:32.770302+00:00`
- Summary JSON: `output/trusts_extraction/ch_probe_25pct_20260218T165602Z/summary.json`
- Total CH requests made: `16`
- Requests per trust in this lean probe: `~4`
  - profile
  - filing history page
  - document metadata
  - document download

## Outcomes

All 4 trusts succeeded at Companies House stages through document download.

| Company Number | Outcome | Stage Reached | Document ID |
|---|---|---|---|
| 09610951 | Success | `download_ok` | `Z1bmbZejG_zbsaRudyI30P1VMrLoBcuNb6Up4-GJkf4` |
| 09174628 | Success | `download_ok` | `O7ooiBUkf0La-pXSWCgcfnxmsxvjtfLjiKvurnsTc2A` |
| 08164889 | Success | `download_ok` | `jr1kxzk-K1T-P6rxqZWUmfLVGdz38GGw6ysWlqP7tbE` |
| 08160034 | Success | `download_ok` | `27jA6RuGyf1PuWVeRix4W_hmiJzSZSSSJQD9L8UMrIg` |

## Conclusion

This result supports your hypothesis: the prior `429` failures were driven by request pacing/volume in the earlier workflow, rather than trust-specific data issues.

The previous pipeline path for latest document discovery can trigger many metadata calls per trust. Under load, that can breach practical per-window limits and produce rate rejection.

