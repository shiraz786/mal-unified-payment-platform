from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

out = '/mnt/data/mal_payment_platform/Migration_Architecture_Strategy.pdf'

doc = SimpleDocTemplate(out, pagesize=A4, rightMargin=1.5*cm, leftMargin=1.5*cm, topMargin=1.3*cm, bottomMargin=1.3*cm)
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name='Title2', parent=styles['Title'], fontSize=16, leading=20, alignment=TA_LEFT, spaceAfter=8))
styles.add(ParagraphStyle(name='H2', parent=styles['Heading2'], fontSize=11.5, leading=14, textColor=colors.HexColor('#0F172A'), spaceBefore=6, spaceAfter=4))
styles.add(ParagraphStyle(name='Body2', parent=styles['BodyText'], fontSize=8.7, leading=11.2, spaceAfter=4))
styles.add(ParagraphStyle(name='Small', parent=styles['BodyText'], fontSize=8.1, leading=10.2, spaceAfter=3))

story = []
add = story.append

add(Paragraph('Mal Senior Data Engineer Assessment - Architecture & Migration Strategy', styles['Title2']))
add(Paragraph('Context: shared payment data platform across Cards, Transfers, and Bill Payments for an AI-native Islamic digital finance platform in the UAE.', styles['Body2']))

sections = [
('1. Canonical Entity Design Rationale', [
"I chose a single <b>canonical payment event</b> because operations, analytics, customer support, risk, and product teams should not need three different payment models for basic questions like: what happened, for whom, for how much, when, and on which rail. The model keeps a strong shared core: <font name='Courier'>event_id, payment_type, customer_id, amount, currency, event_timestamp, status, payment_method</font>. These are the fields most downstream consumers will always need.",
"To handle extensibility, the schema uses a <b>common spine plus nullable rail-specific attributes</b>. For example, card payments can populate <font name='Courier'>merchant_name</font> and <font name='Courier'>card_last4</font>, transfers can populate source and destination accounts, and bill payments can populate <font name='Courier'>biller_name</font> and invoice reference. This avoids forcing every squad into a completely separate model while still leaving room for future rails such as wallet top-ups, salary disbursements, or zakat-related payments.",
"I also included <b>platform fields</b>: <font name='Courier'>raw_source</font>, <font name='Courier'>contract_version</font>, and <font name='Courier'>ingestion_ts</font>. These are not business-facing, but they are critical for migration safety, auditability, and debugging during a multi-team rollout. In Mal's context, where trust, transparency, and ethical behavior matter, lineage is not optional; it is part of the product promise.",
"The key trade-off was <b>simplicity over full completeness</b>. I intentionally did not model fees, FX rates, partial captures, chargeback lifecycles, settlement states, Shariah product flags, or event sourcing semantics in Part 1. Those are real-world needs, but including all of them now would make the shared model harder to adopt. For an early cross-squad platform, a simpler model that teams actually use is better than a perfect model that nobody adopts."
]),
('2. Phased Migration Plan', [
"<b>First 30 days - align and prove value.</b> Start with Cards first because card events usually have higher volume and richer merchant context, which makes the value of a unified model easy to demonstrate. Freeze a <b>v1 shared contract</b>, define mandatory fields, and publish mapping rules from each squad's local schema to the canonical entity. Run the unified pipeline in shadow mode beside existing squad pipelines. The goal is not to cut over immediately; the goal is to produce the same answers consistently and expose schema gaps early.",
"<b>Days 31-60 - onboard Transfers and backward compatibility.</b> Add Transfers next because account-to-account and interbank flows are important for customer operations and reconciliation. During this phase, maintain <b>dual-read compatibility</b>: downstream users can keep using existing squad outputs while the platform publishes the unified dataset in parallel. For breaking changes, such as adding new mandatory monitoring fields, support migration helpers from v1 to v2 so squads are not blocked by immediate rewrites.",
"<b>Days 61-90 - onboard Bill Payments and operationalize adoption.</b> Bill Payments comes third because it usually introduces provider-specific nuances such as biller references and invoice numbers. By this point, the platform should already have shared validation, standard statuses, and common monitoring. Move reporting, support dashboards, and payment KPI views to the unified model first. Once consumers trust the shared outputs, begin deprecating duplicate squad transforms.",
"<b>Adoption sequence rationale.</b> The order is based on quickest business visibility first, then broader reuse: Cards gives a strong demo, Transfers strengthens core banking operations, and Bill Payments finishes cross-product consistency. <b>Dependency management</b> should be handled through a weekly architecture forum with one owner from the platform team and one tech representative from each squad. Every contract change should include: change summary, expected impact, migration action, and target release date."
]),
('3. Data Contract & Governance', [
"I would use a lightweight versioning model: <b>non-breaking changes</b> (new nullable columns, extra metadata, widened enums) can stay within the same major version, while <b>breaking changes</b> (renamed fields, stricter mandatory fields, changed semantics) require a major version bump, for example v1 to v2. Each contract version should be documented in the repo and validated in code before outputs are published.",
"Validation should be enforced at three points: <b>ingestion</b> (basic shape and type checks), <b>transformation</b> (business rules like positive amounts and allowed statuses), and <b>publish</b> (required output columns and version stamp). Invalid rows should never silently disappear; they should go to a quarantine or error store with reason codes, which is what the sample implementation already demonstrates.",
"Ownership should be clear. The <b>data platform team</b> owns the canonical contract, validation framework, and shared outputs. <b>Product squads</b> own source correctness and mapping logic from their local events into the shared schema. <b>Approvals</b> for breaking contract changes should require both platform approval and at least one downstream consumer sign-off, so the shared model stays useful and not just technically elegant."
]),
('4. Adoption Metrics & Stakeholder Plan', [
"I would track five simple KPIs. <b>(1) Contract adoption rate</b>: percent of payment flows publishing through the canonical schema. <b>(2) Reuse rate</b>: number of downstream dashboards or jobs reading the unified model instead of squad-specific outputs. <b>(3) Data quality pass rate</b>: valid records divided by total ingested records. <b>(4) Time to onboard a new payment type</b>: a platform effectiveness measure. <b>(5) Reduction in duplicate transformations</b>: how many squad-owned custom mappings can be retired.",
"For communication, I would keep it practical, not theoretical. Start with a short architecture note, a mapping template, a shared Slack or Teams channel, and fortnightly demos showing real examples: one failed payment operations view, one customer 360 view, and one product KPI dashboard built on the canonical model. People support what they can see working.",
"Resistance is normal, especially from squads that already have functioning pipelines. I would handle that by reducing their workload, not by arguing architecture purity. The message should be: the platform team will provide mapping helpers, version migration support, and shared validation so squads can move faster with less maintenance. For teams worried about loss of control, keep rail-specific fields visible and allow a transition period where local outputs continue until confidence is established."
]),
('5. Production Considerations', [
"At <b>100K transactions per day</b>, the design still works, but I would add orchestration, idempotent loads, partitioned storage, automated tests, and warehouse serving tables. Parquet remains a good storage format, but in production I would land bronze raw events first, then publish a validated silver canonical table, and optionally a gold analytics layer for product and finance reporting.",
"For <b>monitoring and alerting</b>, I would track freshness, volume anomalies by payment type, error-rate spikes, null-rate drift on mandatory fields, and status distribution changes. Alerts should route differently depending on the failure: source ingestion problems to squad owners, contract issues to platform engineering, and business anomalies to operations or finance.",
"What I intentionally cut from Part 1: streaming ingestion, CDC, schema registry tooling, CI/CD, secrets management, PII tokenization, settlement and reconciliation models, and advanced Islamic finance attributes. I cut these on purpose to keep the submission focused on the core platform problem: creating one reusable payment foundation that multiple squads can adopt quickly without heavy process overhead."
]),
]

for title, paragraphs in sections:
    add(Paragraph(title, styles['H2']))
    for p in paragraphs:
        add(Paragraph(p, styles['Body2']))

add(Spacer(1, 6))
add(Paragraph('Suggested KPIs Snapshot', styles['H2']))

tbl = Table([
    ['KPI', 'Why it matters'],
    ['Contract adoption rate', 'Shows whether squads are actually moving to the shared platform'],
    ['Data quality pass rate', 'Shows trustworthiness of the unified dataset'],
    ['Reuse rate', 'Measures whether downstream teams are avoiding duplicate logic'],
    ['Time to onboard new rail', 'Shows extensibility and team efficiency'],
    ['Duplicate pipelines retired', 'Shows engineering cost reduction and simplification'],
], colWidths=[5.2*cm, 10.3*cm])

tbl.setStyle(TableStyle([
    ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#E2E8F0')),
    ('TEXTCOLOR', (0,0), (-1,0), colors.black),
    ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#CBD5E1')),
    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
    ('FONTSIZE', (0,0), (-1,-1), 8),
    ('LEADING', (0,0), (-1,-1), 9.5),
    ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F8FAFC')])
]))
add(tbl)

doc.build(story)
print(out)
