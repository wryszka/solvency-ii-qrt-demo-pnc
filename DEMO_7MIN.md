# Solvency II QRT Demo — 7-Minute Talk Track

**Company:** Bricksurance SE — mid-size European P&C insurer
**QRTs:** S.05.01, S.06.02, S.25.01, S.19.01, S.26.06
**Audience:** CFO, Chief Actuary, Head of Compliance, IT stakeholders
**Pre-reqs:** App running, pipeline has been run, two architecture diagrams ready

---

## Minute 0–1: The Problem (60s, spoken)

> "It's January. Q4 close. Your actuaries are in Excel — three versions of the same
> workbook, nobody sure which is current. The risk function can't see the latest SCR
> numbers because they're on someone's laptop. Your QRT deadline is 8 weeks away and
> you're spending half of that on data wrangling and reconciliation.
>
> I'm going to show you the entire QRT cycle — source data to regulator-ready
> submission — running on Databricks, in under 7 minutes."

---

## Minute 1–2:30: Diagram 1 — Simple QRTs (90s)

**Show:** `docs/diagram_simple_qrts.png`

> "Let's start with the straightforward QRTs. S.05.01 — premiums, claims, expenses.
> S.06.02 — list of assets. These are essentially SQL."

Walk left to right through the medallion:

| Layer | What to say |
|-------|-------------|
| **Bronze** | "Raw data lands from your source systems — policy admin, claims, investments. In the demo this is synthetic, but in production these are your actual feeds." |
| **Silver** | "Silver is where we cleanse and enrich. Asset CIC decomposition, Solvency II valuations, line-of-business aggregation. This runs as **Spark Declarative Pipelines** — you get automatic lineage and data quality checks for free." |
| **Gold** | "Gold is just a column rename to EIOPA cell references — C0040, C0050, R0110. That's it. The output is what goes to Tagetik or your XBRL tool." |
| **Governance** | "And wrapping it all — Unity Catalog for access control and lineage, automated DQ expectations, an approval workflow, and a one-click regulatory package." |

**Key message:**

> "An actuary can read this code. It's SQL. No black boxes, no hidden spreadsheet formulas."

---

## Minute 2:30–4: Diagram 2 — Complex QRTs (90s)

**Show:** `docs/diagram_complex_qrts.png`

> "Now the interesting one. S.25.01 — your SCR. S.19.01 — claims triangles.
> S.26.06 — underwriting risk. These need actuarial maths."

Walk through the flow, emphasising the engine handoff:

| Step | What to say |
|------|-------------|
| **Bronze** | "Same pattern — claims triangles, exposure data, reinsurance structures land in bronze." |
| **Silver (prepare)** | "Databricks runs the chain ladder, prepares exposure sets, structures the reinsurance programme. This is the input your stochastic engine needs." |
| **→ Engine** | "Data goes OUT to your actuarial engine — Igloo, RAFM, ReMetrica, whatever you use. It runs 10,000 simulations, calculates VaR, TVaR by peril and line of business." |
| **← Silver (consume)** | "Results come BACK into Databricks. We aggregate using the EIOPA correlation matrix, calculate diversification benefit, and land the SCR modules." |
| **Gold** | "Same gold layer — EIOPA cell references. Same templates as the simple path." |
| **Governance** | "And critically — **identical governance**. Same lineage, same DQ checks, same approval workflow. Whether it's a SQL transform or a stochastic model, the audit trail is the same." |

**Key message:**

> "Databricks doesn't replace your actuarial engine. It orchestrates around it —
> prepares the inputs, consumes the results, and provides the governance layer.
> One platform for both simple and complex QRTs."

---

## Minute 4–7: The App (3 min)

**Switch to:** Browser → Bricksurance SE QRT Dashboard

> "Let me show you what the business user actually sees. No notebooks, no terminal."

### Dashboard (45s)

Open the Dashboard tab.

| KPI | What to say |
|-----|-------------|
| Total Assets (SII) | "~€6.5B in Solvency II valued assets" |
| Gross Written Premium | "€2B across 7 lines of business" |
| SCR / Solvency Ratio | "SCR of ~€1.15B, solvency ratio comfortably above 100%" |
| DQ Checks | "Every number has passed automated quality checks" |

> "This isn't a static report. Re-run the pipeline with updated data and these
> numbers refresh automatically."

### QRT Reports (45s)

Click → QRT Reports tab.

- **S.06.02**: "Every asset mapped to EIOPA cell references. Click Export CSV — that's your Tagetik feed."
- **S.25.01**: "The SCR waterfall. Market risk, underwriting risk, diversification — all calculated, all traceable."

> "Both the SQL QRTs and the actuarial QRTs are here side by side, from the same platform."

### Audit & Lineage (45s)

Click → Audit & Lineage tab.

> "This is what your regulator wants. Every transformation logged — source, target,
> row counts, timestamps, SHA-256 checksums. If an auditor asks 'how was this SCR
> derived?' — you open this tab and trace it back to the source."

- Point out the data quality checks: pass/fail counts, severity levels

### Approvals + Package (45s)

Click → Approvals tab.

> "Before anything leaves the building, it goes through human sign-off. The actuary
> submits, the reviewer checks, approves or rejects with comments."

- Click **Submit for Review** (or show an existing submission)

> "And when approved — one click generates the regulatory package. QRT CSVs, full
> audit trail, data quality evidence. That's your submission."

- Click **Download Regulatory Package** if time allows

---

## Closing (while on the app, last 15s)

> "Your 6-week QRT cycle. Same-day on Databricks. Simple QRTs in SQL, complex QRTs
> with your existing actuarial engine, all governed by one platform. Questions?"

---

## Common Questions (if they come up)

| Question | Answer |
|----------|--------|
| "Can this handle our volumes?" | "The demo is 20K policies. Databricks scales to billions — same architecture, bigger compute." |
| "How do we connect our real systems?" | "Replace the bronze notebooks with connectors to your policy admin, claims, and investment systems." |
| "What about XBRL?" | "Gold tables produce EIOPA-format data. Feed the CSVs to Tagetik, Invoke, or any XBRL tool." |
| "Internal model, not Standard Formula?" | "Same pattern. The engine integration works whether it's SF or IM — the governance layer is identical." |
| "How long to implement?" | "Pipeline structure: 2-3 weeks. The hard part is always source system integration and business rule validation." |
| "IFRS 17?" | "Same architecture. Different templates, different calculations, same medallion pattern." |

---

## Demo Checklist

- [ ] Pipeline run recently (fresh data)
- [ ] App deployed and accessible
- [ ] Two architecture diagrams ready: `docs/diagram_simple_qrts.png`, `docs/diagram_complex_qrts.png`
- [ ] Browser tab open on the app
- [ ] Diagrams ready to show (slides, PDF, or separate browser tabs)
