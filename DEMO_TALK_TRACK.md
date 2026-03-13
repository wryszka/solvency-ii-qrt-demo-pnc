# Solvency II QRT Demo — Talk Track

## About This Demo

**Company:** Bricksurance SE — a mid-size European P&C insurer
**Reporting period:** Q4 2025 (annual)
**QRTs produced:** S.05.01, S.06.02, S.19.01, S.25.01, S.26.06

This demo has two modes:

| Mode | Audience | What You Show | Duration |
|------|----------|---------------|----------|
| **Executive / Actuarial** | CFO, Chief Actuary, Compliance | The App — dashboards, QRT reports, approvals, regulatory export | ~20 min |
| **Technical** | IT, Data Engineering, Architecture | Behind the scenes — notebooks, DAG, Unity Catalog, lineage, DABs | ~30 min |

You can run either mode standalone or combine them (exec first, then "let me show you what's under the hood").

---

# MODE 1: EXECUTIVE / ACTUARIAL DEMO

*For: CFO, Chief Actuary, Head of Compliance, Actuaries*
*Tools: App only (browser), no terminal*

---

## Scene 1 — The Problem (2 min)

**Open with:**

> "Let me paint a picture you'll recognise. It's January, Q4 close. Your actuaries are in
> Excel — multiple versions of the same workbook floating around on email. Someone's updated
> the claims triangle but the SCR team hasn't picked it up yet. The risk function needs to
> sign off but they can't see the latest numbers. Your QRT submission deadline is 8 weeks
> away and you're spending 4 of those on data wrangling and version control.
>
> What if I could show you the entire QRT cycle — from source data to regulator-ready
> submission — running on a single platform, with full audit trail, in under 10 minutes?"

---

## Scene 2 — The Dashboard (3 min)

**Open the App → Dashboard tab**

> "This is Bricksurance SE's QRT reporting dashboard. Everything you see here is live —
> sourced from the same tables that produce the regulatory submissions."

**Walk through the KPIs:**

| KPI | What to Say |
|-----|-------------|
| Total Assets (SII) | "~€6.5B in Solvency II valued assets — government bonds, corporates, equity, CIUs" |
| Gross Written Premium | "€2B across 7 lines of business — motor, property, liability, health" |
| Combined Ratio | "Running at ~96% — profitable underwriting" |
| SCR | "Solvency Capital Requirement of ~€1.15B" |
| Solvency Ratio | "Comfortably above 100% — the board can sleep at night" |
| DQ Checks | "Every number on this screen has passed automated data quality checks. I'll show you those in a moment." |

**Key message:**

> "This isn't a static report. If someone re-runs the pipeline with updated data,
> these numbers refresh automatically. No copy-paste, no broken links."

---

## Scene 3 — QRT Reports (5 min)

**Click → QRT Reports tab**

### S.06.02 — List of Assets

> "This is your S.06.02 — every asset in the investment portfolio, mapped to EIOPA cell
> references. CIC codes, SII valuations, credit quality steps — all derived automatically
> from the investment register."

- Show the column headers match EIOPA template (C0040, C0050, etc.)
- Filter by CIC code or country to show it's interactive
- Click **Export CSV** → "This is what goes to Tagetik or your XBRL tool"

### S.05.01 — Premiums, Claims and Expenses

> "S.05.01 — your P&L by line of business. Gross, reinsurance share, net — all reconciled
> automatically. The pipeline checks that Net = Gross minus Reinsurance to the cent."

- Show the LoB breakdown (motor, property, liability, etc.)
- Point out the total row

### S.25.01 — SCR Standard Formula

> "And here's the SCR waterfall. Market risk, underwriting risk, default risk — aggregated
> using the EIOPA correlation matrix. Diversification benefit, operational risk, LAC
> adjustments — all calculated automatically."

- Walk down the waterfall: modules → BSCR → adjustments → final SCR

**Key message:**

> "Three QRTs, all consistent because they're sourced from the same data lake. No
> reconciliation needed between teams — it's the same pipeline."

---

## Scene 4 — Audit & Lineage (4 min)

**Click → Audit & Lineage tab**

### Lineage View

> "This is what your regulator wants to see. Every transformation — from raw data to
> QRT output — is logged. Source tables, target tables, row counts, timestamps, who
> ran it, and a SHA-256 checksum so you can prove nothing was tampered with."

- Click through lineage rows: setup → bronze generation → silver transforms → gold QRT formatting
- Point out the `transformation_type` column: generation, enrichment, aggregation, formatting

### Data Quality View

> "And here are the automated quality checks. Every step runs validation — null checks,
> reconciliation checks, range checks. If something fails, the pipeline flags it before
> it ever reaches a QRT."

- Show pass/fail counts
- Point out severity levels (critical, warning, info)

### Regulatory Package

> "When you're ready to submit — one click. This downloads a ZIP with all QRT CSVs,
> the full audit trail, and data quality evidence. That's your submission package for
> Tagetik, or directly to the regulator."

- Click **Download Regulatory Package**
- Show the ZIP contents: QRT CSVs + lineage CSV + quality CSV

**Key message:**

> "If an auditor asks 'show me how this SCR number was derived' — you open this tab
> and trace it back to the source data. Every step, every check, every timestamp."

---

## Scene 5 — Approval Workflow (3 min)

**Click → Approvals tab**

> "Before anything goes to the regulator, it goes through an approval workflow.
> The actuary submits, the reviewer checks the numbers, and only then is it approved
> for submission."

- Show **Submit for Review** button
- Click it → status changes to "Pending Review"
- Show the reviewer flow: add comments, Approve or Reject
- "If rejected, the actuary sees the comments, fixes the issue, re-runs, re-submits"

**Key message:**

> "This is your Human-in-the-Loop control. The platform automates the calculation,
> but a qualified person signs off. That's how you satisfy the Actuarial Function
> requirements under Solvency II."

---

## Scene 6 — The Bigger Picture (3 min)

**Closing narrative (no clicking needed):**

> "What you've just seen is the end-to-end QRT cycle:
>
> 1. Data arrives from your source systems — policy admin, investment platform, claims
> 2. The platform transforms it through bronze, silver, gold layers
> 3. QRT templates are populated automatically
> 4. Quality checks run at every step
> 5. A human reviews and approves
> 6. A submission package is generated for Tagetik or XBRL filing
>
> All of this is version-controlled, auditable, and reproducible.
>
> Your 6-week QRT cycle? It's now same-day. And if you need to re-run with a correction,
> it's a single button click — not a 3-day scramble through email chains."

**If asked about complex QRTs (internal models / Igloo / RAFM):**

> "For undertakings using an internal model — say you run Igloo or RAFM for your
> catastrophe risk — the pattern is the same. Databricks prepares the input data,
> hands it off to the actuarial model, picks up the results, and feeds them into
> the same QRT templates. The orchestration, governance, and audit trail are identical.
> We have a prototype of that integration ready to show if you're interested."

---

# MODE 2: TECHNICAL DEMO

*For: Data Engineers, IT Architects, Actuarial Modellers*
*Tools: Databricks UI + Terminal + App*

---

## Scene 1 — Architecture (3 min)

**Show the pipeline DAG in Workflows:**

> "This is a Databricks Asset Bundle — the entire pipeline is defined in YAML and
> version-controlled in Git. Let me show you the workflow."

**Go to: Workflows > Jobs > `[dev] solvency_ii_qrt_pnc_pipeline`**

- Click into the job → show the DAG visualization
- Point out the parallel branches:

```
setup → bronze_data →  silver_assets        → gold_s0602
                    →  silver_premiums/claims → gold_s0501  → validation
                    →  silver_scr            → gold_s2501
```

> "Bronze generates the synthetic data. Silver runs three parallel branches —
> assets, P&C aggregation, and SCR calculation. Gold formats into EIOPA templates.
> Validation cross-checks everything."

**Key message:**

> "Parallel execution. If one branch fails, the others still complete. The validation
> task only runs when all three gold tables are ready."

---

## Scene 2 — Data Layer (5 min)

**Go to: Catalog > `main` (or your catalog) > `solvency2demo`**

> "Everything lives in Unity Catalog. Let me walk you through the medallion layers."

### Bronze tables

- Click `bronze_assets` → show schema, sample data
- "5,000 assets with CIC codes, ISIN, market values, counterparty info"
- Click `bronze_policies` → "20,000 policies across 7 lines of business"

### Silver tables

- Click `silver_assets_enriched` → show the CIC decomposition columns, SII asset class mapping
- "The silver layer adds derived fields — CIC breakdown, risk flags, SII valuation adjustments"
- Click `silver_scr_modules` → "This is the SCR waterfall — every module, sub-module, the correlation matrix calculation"

### Gold tables

- Click `gold_qrt_s0602` → show EIOPA cell references as column names
- "Gold is purely formatting — mapping silver fields to EIOPA template cell references"

### Lineage

- Click the **Lineage** tab on any table → show Unity Catalog's built-in lineage graph
- "Unity Catalog tracks lineage automatically. Combined with our custom audit trail, you have both platform-level and business-level traceability."

---

## Scene 3 — Simple QRTs: The SQL Path (5 min)

**Open notebook `06_gold_s0501.py` in the workspace**

> "Let me show you what a 'simple' QRT looks like under the hood. S.05.01 — premiums,
> claims, expenses by line of business."

- Scroll through the notebook
- Point out: "This is pure SQL/PySpark. Read from silver, map to EIOPA rows, write to gold."
- Show the EIOPA row mapping (R0110 = Gross Written Premium, R0160 = Net Written Premium, etc.)
- Show the lineage logging call at the end

> "An actuary can read this. The mapping from business data to regulatory template is
> explicit and auditable. No hidden spreadsheet formulas."

**Open notebook `05_gold_s0602.py`**

> "S.06.02 is even simpler — it's essentially a SELECT with column renames. Each column
> maps to an EIOPA cell reference."

---

## Scene 4 — Standard Formula: The Actuarial Path (5 min)

**Open notebook `04_silver_scr.py`**

> "Now let's look at something more actuarial. This is the SCR Standard Formula
> calculation."

Walk through:
1. "Sub-module charges come from bronze — interest rate, equity, property, spread, currency, concentration"
2. "We apply the EIOPA 5×5 correlation matrix" — show the matrix in the code
3. "The aggregation uses the square root formula: BSCR = √(Σᵢ Σⱼ Corrᵢⱼ × SCRᵢ × SCRⱼ)"
4. "Then operational risk, LAC adjustments, and the final SCR"

> "This is deterministic and reproducible. Same inputs, same outputs, every time.
> The audit trail captures the parameters so you can prove what was used."

**Open notebook `21_silver_chain_ladder.py`**

> "For reserving, we have the chain-ladder method. Development triangles, link ratios,
> ultimate claims — feeding into S.19.01 and S.26.06."

---

## Scene 5 — Complex QRTs: External Model Integration (4 min)

> "Now, what about undertakings with an internal model? You're running Igloo, RAFM,
> or ReMetrica for your catastrophe risk. How does that fit?"

**Explain the pattern (with diagram or whiteboard):**

```
Databricks                      External Model
─────────                       ──────────────
Silver data ──── prepare ────→  Exposure sets (CSV/Parquet)
                                Reinsurance structures
                                     │
                                     ▼
                                Igloo / RAFM / ReMetrica
                                (runs externally)
                                     │
                                     ▼
Results     ←── pick up ─────  VaR, TVaR, percentiles
(bronze_igloo_results)              by peril & LoB
     │
     ▼
Silver transforms ──→ Gold QRT (same S.25.01 template,
                               internal model figures)
```

> "Databricks doesn't replace your actuarial model. It orchestrates the workflow:
> prepares the inputs, waits for results, and feeds them into the same QRT templates.
> The governance layer — lineage, quality checks, approvals — is identical.
>
> In production, this handoff can be automated via APIs, file watches on cloud
> storage, or scheduled triggers."

**[If notebooks 30-33 are built, show them live. Otherwise, describe the pattern.]**

---

## Scene 6 — Automation & CI/CD (3 min)

**Open terminal:**

```bash
cd solvency-ii-qrt-demo-pnc
cat databricks.yml
```

> "The entire pipeline is defined as a Databricks Asset Bundle. This YAML plus the
> notebooks in `src/` — that's everything. It's in Git."

```bash
git log --oneline -5
```

> "Every change is tracked. If a regulator asks 'what changed between Q3 and Q4
> submissions?' — it's a git diff."

**Show the deploy flow:**

```bash
databricks bundle validate -t dev    # catch errors before deployment
databricks bundle deploy -t dev      # deploy to workspace
databricks bundle run qrt_pipeline -t dev   # trigger the pipeline
```

> "Three commands: validate, deploy, run. In a CI/CD pipeline, this happens
> automatically on every merge to main."

**Cleanup:**

```bash
databricks bundle destroy -t dev --auto-approve   # remove everything
```

> "And if you need to tear it all down — one command. No orphaned resources."

---

## Scene 7 — The App: Tying It Together (5 min)

**Switch to the App (browser)**

> "Everything we just looked at behind the scenes is surfaced through this app.
> The actuarial team never needs to touch a notebook or a terminal."

- **Dashboard** → "Executive KPIs, live from the gold tables"
- **QRT Reports** → "Browse and export any QRT — this is what feeds Tagetik"
- **Audit & Lineage** → "Full traceability — every step, every check"
- **Approvals** → "Human-in-the-loop sign-off before submission"
- **Regulatory Package** → "One-click ZIP: QRTs + audit evidence"

> "The app is a Databricks App — it runs inside the workspace, inherits the
> governance model, and uses Unity Catalog for all data access. No separate
> infrastructure to manage."

---

# COMMON QUESTIONS

| Question | Answer |
|----------|--------|
| "Can this handle our actual data volumes?" | "The synthetic demo generates 20K policies and 60K transactions. Databricks scales to billions of rows — the architecture is the same, just bigger clusters." |
| "How do we connect to our real source systems?" | "Replace the bronze generation notebook with connectors to your policy admin, claims, and investment systems. Databricks has native connectors for Oracle, SAP, Guidewire, etc." |
| "What about XBRL filing?" | "The gold tables produce EIOPA-format data. Feed the CSV exports to your XBRL tool (Tagetik, Invoke, etc.) or build an XBRL renderer — the data structure is ready." |
| "Can actuaries modify the calculations?" | "Yes. The notebooks are readable Python/SQL. Version control means every change is tracked and reversible." |
| "What about IFRS 17?" | "Same architecture. Different templates, different calculations, same medallion pattern and governance layer." |
| "How long to implement?" | "The pipeline structure takes 2-3 weeks. The hard part is always source system integration and business rule validation — same as any project." |
| "What if the regulator changes the template?" | "Update the gold notebook mappings. The bronze and silver layers don't change. Deploy and re-run." |
| "How does this compare to Tagetik / Invoke / Regnology?" | "We're not replacing your disclosure management tool. We're replacing the Excel-and-email data pipeline that feeds it. Databricks produces clean, reconciled data — your QRT tool does the final formatting and filing." |
| "What about internal models and partial internal models?" | "Same orchestration pattern. Databricks prepares inputs for Igloo/RAFM/ReMetrica, picks up results, feeds them into the QRT templates. We have a prototype of that integration." |

---

# DEMO CHECKLIST

### Before the demo

- [ ] Pipeline has been run recently (data is fresh)
- [ ] App is deployed and accessible
- [ ] Browser tabs pre-opened: App, Databricks Workflows, Catalog Explorer
- [ ] Terminal ready in the project directory (for tech demo)

### Quick reset between demos

```bash
# Re-run the pipeline to get fresh timestamps
databricks bundle run qrt_pipeline -t dev

# If you need a full reset (drops and recreates everything)
databricks bundle run qrt_pipeline -t dev
```

### Full cleanup after demo

```bash
databricks bundle destroy -t dev --auto-approve
```
