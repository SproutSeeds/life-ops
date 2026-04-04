## FRG BIMI Rollout

Canonical sender logo:

- `https://frg.earth/branding/frg-bimi-iris-floating.svg`

Current live DNS state checked on March 31, 2026:

- `_dmarc.frg.earth = "v=DMARC1; p=none;"`
- `default._bimi.frg.earth` is not published yet

Current live asset state:

- the canonical SVG is publicly reachable at `https://frg.earth/branding/frg-bimi-iris-floating.svg`

Important Gmail constraints:

- Gmail requires BIMI plus a hosted PEM for the official inbox logo
- Gmail requires a `VMC` or `CMC`
- BIMI does not work for Gmail with `DMARC p=none`
- Google recommends a solid background; the floating iris asset is the chosen canonical mark, but its transparent background may render less consistently than a solid-square treatment

Recommended DNS target values:

### DMARC

Host:

- `_dmarc.frg.earth`

TXT value:

```txt
v=DMARC1; p=quarantine; pct=100;
```

### BIMI

Host:

- `default._bimi.frg.earth`

TXT value after the certificate is issued and hosted:

```txt
v=BIMI1; l=https://frg.earth/branding/frg-bimi-iris-floating.svg; a=https://frg.earth/branding/frg-bimi.pem
```

Certificate path to host once issued:

- `https://frg.earth/branding/frg-bimi.pem`

What is already done:

- canonical logo chosen
- canonical logo hosted on `frg.earth`
- asset is within Google's recommended `32 KB` size guidance
- SVG uses `version="1.2"` and `baseProfile="tiny-ps"`
- exact DNS values are prepared

What is still required:

1. Purchase a `CMC` or `VMC`
2. Host the issued PEM at `/branding/frg-bimi.pem`
3. Change DMARC from `p=none` to `p=quarantine`
4. Publish the BIMI TXT record
5. Wait for mailbox-provider propagation and cache refresh

Practical recommendation:

- Use a `CMC` first unless the FRG mark is already trademarked and you specifically want the higher-assurance `VMC` path
- Keep one stable sender logo for `cody@frg.earth`

References:

- Google BIMI setup: `https://support.google.com/a/answer/10911320`
- Google BIMI SVG requirements: `https://support.google.com/a/answer/10911027`
- BIMI Group issuer list: `https://bimigroup.org/vmc-issuers/`
