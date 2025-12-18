# DDL and PySpark Generation Workflow

**Date**: December 18, 2025
**Status**: Implementation Ready
**Version**: 3.0 (Multi-Signal Table Discovery)

> **Important Update (v3.0)**: The source table discovery approach has been completely redesigned.
> The old Cartesian product approach (generating 31K-100K combinations) has been replaced with
> **Multi-Signal Table Discovery** using graph pathfinding. See [MULTI_SIGNAL_TABLE_DISCOVERY.md](./MULTI_SIGNAL_TABLE_DISCOVERY.md)
> for the detailed design of the new discovery algorithm.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Research Findings](#2-research-findings)
3. [Design Rules & Conventions](#3-design-rules--conventions)
4. [Requirements Summary](#4-requirements-summary)
5. [High-Level Architecture](#5-high-level-architecture)
6. [Detailed Workflow](#6-detailed-workflow)
7. [Scoring System](#7-scoring-system)
8. [State Schema](#8-state-schema)
9. [Node Specifications](#9-node-specifications)
10. [HITL Interrupt Nodes](#10-hitl-interrupt-nodes)
11. [Workflow Graph Updates](#11-workflow-graph-updates)
12. [Validation Rules](#12-validation-rules)
13. [Neo4j Query Reference](#13-neo4j-query-reference)
14. [Streamlit UI Components](#14-streamlit-ui-components)
15. [Implementation Phases](#15-implementation-phases)
16. [Test Scenarios](#16-test-scenarios)
17. [Error Handling](#17-error-handling)

---

## 1. Executive Summary

This document defines the complete workflow for generating **DDL (Data Definition Language)** statements and **PySpark scripts** for Databricks. The workflow integrates with the existing NL-to-SQL agent as a **separate branch** (not replacement) and leverages the Neo4j ontology for schema discovery, lineage tracking, and JOIN condition identification.

### Key Capabilities
- Create new Silver tables (from Bronze/Silver sources)
- Create new Gold tables (from Bronze/Silver/Gold sources)
- Modify existing tables (add/drop columns)
- Generate SQL DDL or PySpark code based on user preference
- Audience-aware output (Business User vs Data Engineer)
- Multi-option recommendation with scoring
- Impact analysis and lineage visualization
- Ontology auto-sync after execution

### What's New in Version 3.0

#### Concept Extraction (v2.0 - Hierarchical)
- **Two-Pass Hierarchical Extraction** - Column concepts nested under parent table concepts
  - Pass 1: Extract table_concepts (business entities)
  - Pass 2: For each table_concept, extract child column_concepts
  - Pass 3: Identify shared_columns (ambiguous, no clear parent)
- **Research-backed approach** - Inspired by RSL-SQL, MAC-SQL, AmbiSQL (2024-2025)

#### Multi-Signal Discovery (Per-Concept)
- **Per-Concept Signal Discovery** - Each table concept gets its own anchor table
  - Column semantic search (35% weight) - searches ONLY that concept's child columns
  - Table semantic search (30% weight) - searches the concept name
  - IS_SOURCE_OF lineage patterns (35% weight)
- **Guaranteed concept coverage** - Every user concept represented by an anchor
- **shared_columns handling** - Ambiguous columns searched across all anchors

#### Path Discovery & Scoring
- **Graph pathfinding** - Neo4j traversal instead of enumeration (5-20 paths vs 31K-100K combinations)
- **Path Scoring** - Column coverage (30%), Layer Priority (25%), Simplicity (25%), Pattern match (20%)

#### Workflow Features
- **Multi-option recommendations** (Top 3 scored options)
- **Feedback loop** (Max 5 iterations)
- **Flexible layer rules** (Gold can source from Bronze/Silver/Gold)
- **Three HITL gates** (Option selection, Code approval, Execution approval)
- **Ontology sync & validation** after successful execution

#### Edge Case Handling (NEW in v3.0)
Robust handling for 6 edge cases:
| Edge Case | Strategy |
|-----------|----------|
| No table_concepts extracted | Infer from columns → Ask user |
| Concept with no signal matches | Partial results + warning |
| Tie-breaking for identical scores | Secondary sort by path_length |
| Invalid target_layer | Validate at entry |
| Same table anchor for multiple concepts | Allow + info message |
| Concept's columns not in anchor | Early warning |

> **See**: [MULTI_SIGNAL_TABLE_DISCOVERY.md](./MULTI_SIGNAL_TABLE_DISCOVERY.md) for detailed algorithm design.

---

## 2. Research Findings

### 2.1 Ontology JSON Analysis

#### Table Mappings (33 total)
| Pattern | Count | Description |
|---------|-------|-------------|
| Bronze → Silver | 13 | Single/multi-source transformations |
| Silver → Gold | 16 | Complex multi-table joins |
| Multi-source | 19 | Tables with 2+ source tables |

**Key Insight**: Gold tables often join 5-7 Silver tables. The `table_mappings` structure provides the complete lineage for any derived table.

#### Sample Multi-Source Gold Table
```json
{
  "destination_table": ["main.e2ematflow_gold.v_e2e_sap_ag_mf_po", "gold"],
  "source_tables": [
    ["main.e2ematflow_silver.e2e_sap_dn_po", "silver"],
    ["main.e2ematflow_silver.e2e_sap_matr", "silver"],
    ["main.e2ematflow_silver.e2e_sap_matr_type", "silver"],
    ["main.e2ematflow_silver.e2e_sap_plant", "silver"],
    ["main.e2ematflow_silver.e2e_sap_po_type", "silver"],
    ["main.e2ematflow_silver.e2e_sap_vend", "silver"],
    ["main.e2ematflow_silver.e2e_sap_vend_type", "silver"]
  ]
}
```

#### Column Mappings (636 total)
| Transformation Type | Count |
|---------------------|-------|
| direct_copy | 636 |

**Note**: All current mappings are direct copies. Future versions may include transformations.

```json
{
  "src_table": "main.e2ematflow_bronze_csv.afpo",
  "src_column": "AMEIN",
  "dst_table": "main.e2ematflow_silver.e2e_sap_wol",
  "dst_column": "AMEIN",
  "transformation_type": null,
  "transform_expression": null
}
```

### 2.2 Relationships Analysis

#### Relationship Distribution
| Type | Count | Layer Usage |
|------|-------|-------------|
| Bronze-Bronze | 15 | For creating Silver tables |
| Silver-Silver | 15 | For creating Gold tables |
| Cross-layer | 9 | Reference tables |

#### Bronze-Bronze Relationships (for Silver Creation)
```
RELATED_TO: ekko.ebeln = ekpo.ebeln
RELATED_TO: mara.MATNR = ekpo.MATNR
RELATED_TO: ekko.lifnr = lfa1.LIFNR
RELATED_TO: t001w.WERKS = ekpo.WERKS
```

#### Silver-Silver Relationships (for Gold Creation)
```
JOINED_ON: e2e_sap_pol.EBELN = e2e_sap_poh.EBELN
JOINED_ON: e2e_sap_dn_po.MATNR = e2e_sap_matr.MATNR
JOINED_ON: e2e_sap_dn_so.KUNNR = e2e_sap_cust.KUNNR
JOINED_ON: e2e_sap_matr_seg.WERKS = e2e_sap_plant.WERKS
```

### 2.3 Neo4j Capabilities for DDL Generation

#### Available Data
| Entity | Count | Embeddings | Use for DDL |
|--------|-------|------------|-------------|
| Table nodes | 49 | Yes | Schema discovery |
| Column nodes | 1835 | Yes | Column definitions |
| HAS_COLUMN | 1835 | - | Table-column mapping |
| JOINED_ON | 30 | Yes | JOIN conditions |
| RELATED_TO | 11 | Yes | JOIN conditions |
| INNER | 1 | Yes | JOIN conditions |
| LEFT_OUTER | 5 | Yes | JOIN conditions |
| IS_SOURCE_OF (Table→Table) | 92 | No | **Lineage patterns, source discovery** |
| IS_SOURCE_OF (Column→Column) | 548 | No | **Column lineage, transformation tracking** |

#### IS_SOURCE_OF Lineage Relationships (NEW)

The `IS_SOURCE_OF` relationship tracks data lineage from Bronze → Silver → Gold layers.

**Table Lineage Properties:**
```cypher
(:Table)-[:IS_SOURCE_OF {
  source_layer: "bronze",      -- Source table layer
  destination_layer: "silver", -- Destination table layer
  lineage_type: "table"        -- Always "table"
}]->(:Table)
```

**Column Lineage Properties:**
```cypher
(:Column)-[:IS_SOURCE_OF {
  src_table: "e2ematflow_bronze_csv.ekpo",
  dst_table: "e2ematflow_silver.e2e_sap_pol",
  src_layer: "bronze",
  dst_layer: "silver",
  lineage_type: "column",
  transformation_type: "direct",     -- Optional: direct, aggregation, etc.
  transform_expression: null         -- Optional: SQL expression
}]->(:Column)
```

**Use Cases for DDL Generation:**
- **Pattern Discovery**: Find proven source combinations for target layer
- **Scoring Boost**: Combinations matching existing lineage patterns score higher
- **Impact Analysis**: Trace full lineage path (Bronze→Silver→Gold)
- **Validation**: Warn if new lineage deviates from established patterns

#### Table Node Schema (Updated)

```cypher
(:Table {
  id: "e2ematflow_bronze_csv.lfa1_2",
  name: "lfa1",                                    -- Short table name
  full_name: "main.e2ematflow_bronze_csv.lfa1",   -- Full qualified name (catalog.schema.table)
  catalog: "main",                                 -- Catalog name (configurable per project)
  schema: "e2ematflow_bronze_csv",                 -- Schema name
  layer: "bronze",                                 -- Data layer: bronze | silver | gold
  description: "Vendor master data...",            -- AI-generated description
  description_embedding: [1536 floats]             -- OpenAI embedding for semantic search
})
```

| Property | Type | Description | Use Case |
|----------|------|-------------|----------|
| `catalog` | string | Database catalog (e.g., "main") | Build full table path for SQL |
| `schema` | string | Schema name | Identify layer, build path |
| `layer` | string | bronze/silver/gold | Filter by layer for DDL rules |
| `full_name` | string | catalog.schema.table | Complete reference for queries |

**Indexes for Efficient Filtering:**
```cypher
CREATE INDEX table_layer_idx FOR (t:Table) ON (t.layer)
CREATE INDEX table_catalog_idx FOR (t:Table) ON (t.catalog)
CREATE INDEX table_schema_idx FOR (t:Table) ON (t.schema)
```

#### Semantic Search Capabilities
- **Table discovery**: Find relevant tables by natural language
- **Column discovery**: Find relevant columns per table
- **Relationship discovery**: Find JOIN conditions between tables
- **Layer filtering**: Filter tables by layer (bronze/silver/gold) for DDL rules

---

## 3. Design Rules & Conventions

### 3.1 Naming Conventions

| Element | Format | Example |
|---------|--------|---------|
| Table (full) | `catalog.schema.tablename` | `main.e2ematflow_silver.e2e_sap_vend` |
| Catalog | `main` | Fixed |
| Schema (Bronze) | `e2ematflow_bronze_csv` | Bronze layer |
| Schema (Silver) | `e2ematflow_silver` | Silver layer |
| Schema (Gold) | `e2ematflow_gold` | Gold layer |
| Column | Direct name | `LIFNR`, `MATNR` |
| Table Alias | `T1`, `T2`, `T3`... | Sequential |

### 3.2 Layer Rules (Updated)

| Target Layer | Can Source From | Notes |
|--------------|-----------------|-------|
| Silver | Bronze, Silver | Flexible sourcing |
| Gold | Bronze, Silver, Gold | Most flexible |

**Change from v1**: Originally Silver could only source from Bronze, Gold only from Silver. Now both are more flexible to support real-world patterns.

### 3.3 JOIN Rules (Critical)

| Rule | Description |
|------|-------------|
| **Source** | Only use relationships that exist in Neo4j ontology |
| **Forbidden** | SAP recommended tables NOT in our ontology |
| **Validation** | Every JOIN must exist in Neo4j relationships |
| **No Fabrication** | System cannot invent JOINs not in ontology |

### 3.4 Output Audience

| Audience | Output Style |
|----------|--------------|
| Business User | Simplified SQL with comments explaining business logic |
| Data Engineer | Full PySpark/SQL with technical details, error handling |

---

## 4. Requirements Summary

| Requirement | Decision |
|-------------|----------|
| Visualization | Streamlit (matplotlib/networkx) |
| Max Options Shown | 3 |
| Max Feedback Iterations | 5 |
| Column Modification After Approval | No (use feedback instead) |
| Auto-Execute After Approval | No (require explicit execution approval) |
| Ontology Update | Yes, after successful execution |
| Ontology Validation | Yes, validate no breaks after update |

### Open Questions (Resolved)

| Question | Resolution |
|----------|------------|
| Should DDL be auto-executed? | No, require explicit approval |
| Should system suggest table names? | Yes, but user can override |
| How to handle complex transformations? | Future enhancement |
| CREATE OR REPLACE vs IF NOT EXISTS? | Use CREATE OR REPLACE |
| Should new tables be added to ontology? | Yes, automatically after execution |

---

## 5. High-Level Architecture

### CRITICAL: DDL Flow is a BRANCH, Not a Replacement

The DDL/PySpark workflow is a **separate branch** from the existing SQL generation flow. The existing flow remains unchanged.

```
                                    User Query
                                         │
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │        EXISTING: query_classifier_node      │
                    │  ───────────────────────────────────────────│
                    │  Classifies intent into one of 8 categories │
                    └─────────────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
    ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
    │ EXISTING FLOW   │      │ EXISTING FLOW   │      │   NEW FLOW      │
    │ ─────────────── │      │ ─────────────── │      │ ─────────────── │
    │ metadata        │      │ simple_sql      │      │ ddl_generation  │
    │ follow_up       │      │ complex_sql     │      │ pyspark_gen     │
    │ visualization   │      │ genealogy       │      │                 │
    └────────┬────────┘      └────────┬────────┘      └────────┬────────┘
             │                        │                        │
             ▼                        ▼                        ▼
    ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
    │ metadata_node   │      │ parse_intent    │      │ ddl_intent_     │
    │ format_results  │      │ fetch_joins     │      │ parser_node     │
    │      END        │      │ generate_sql    │      │      │          │
    └─────────────────┘      │ validate_sql    │      │      ▼          │
                             │ human_approval  │      │ DDL Pipeline    │
                             │ execute_sql     │      │ (12 nodes)      │
                             │ format_results  │      │      │          │
                             │      END        │      │      ▼          │
                             └─────────────────┘      │     END         │
                                                      └─────────────────┘

    ════════════════════════════════════════════════════════════════════
                    EXISTING FLOWS UNCHANGED
    ════════════════════════════════════════════════════════════════════
```

### Intent Categories (Updated)

```python
intent: Literal[
    # Existing (unchanged)
    "metadata",           # Schema questions
    "simple_sql",         # Basic SELECT queries
    "complex_sql",        # Multi-table JOINs
    "follow_up",          # Continuation
    "visualization",      # Charts/graphs
    "genealogy",          # Batch lineage

    # NEW
    "ddl_generation",     # CREATE/ALTER/DROP table DDL
    "pyspark_generation"  # PySpark script generation
]
```

---

## 6. Detailed Workflow

### DDL Generation Pipeline (12 Nodes)

```
User: "Create a silver table with PO and material details"
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ NODE 1: ddl_intent_parser_node                                      │
│ ────────────────────────────────────────────────────────────────────│
│ Input: user_query                                                   │
│ Output: ddl_action, target_layer, output_format, audience,          │
│         requested_concepts, target_table_name (optional)            │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ NODE 2: multi_signal_discovery_node (v3.0)                          │
│ ────────────────────────────────────────────────────────────────────│
│ - Per-concept multi-signal discovery (column/table/lineage)         │
│ - Graph pathfinding between concept anchors                         │
│ - Edge case handling (errors → format_ddl_result,                   │
│                       clarification → concept_clarification_gate)   │
│ - Output: discovered_paths, concept_anchors, discovery_warnings     │
└─────────────────────────────────────────────────────────────────────┘
          │
          ├── error ──────────────────► format_ddl_result (with error message)
          │
          ├── needs_clarification ───► concept_clarification_gate ──┐
          │                                                         │
          │◄──────────────────────────── (with user response) ──────┘
          │
          ▼ success
┌─────────────────────────────────────────────────────────────────────┐
│ NODE 3: combination_scorer_node                                     │
│ ────────────────────────────────────────────────────────────────────│
│ Score each path (v3.0 weights):                                     │
│ - Column Coverage (30%): Do requested columns exist in path?        │
│ - Layer Priority (25%): Prefer higher maturity (Gold > Silver)      │
│ - Simplicity (25%): Fewer joins = better                            │
│ - Pattern Match (20%): Does IS_SOURCE_OF exist for this combo?      │
│ Output: Top 3 scored paths with explanations                        │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ NODE 4: recommendation_presenter_node                               │
│ ────────────────────────────────────────────────────────────────────│
│ Format each option with:                                            │
│ - Score breakdown (Layer Priority, Completeness, Consistency)       │
│ - Source tables and layers                                          │
│ - JOIN conditions                                                   │
│ - Column selections with descriptions                               │
│ - Mini lineage preview                                              │
└─────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ HITL 1: option_selection_interrupt                                  │
│ ────────────────────────────────────────────────────────────────────│
│ User sees 3 options and can:                                        │
│ (A) Select Option 1/2/3 → Continue                                  │
│ (B) Select Option + Feedback → Refine that option                   │
│ (C) Reject All + Feedback → Regenerate (max 5 iterations)           │
└─────────────────────────────────────────────────────────────────────┘
                    │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
   [Rejected]            [Selected]
        │                     │
        │ (iteration < 5)     │
        │                     ▼
        │         ┌───────────────────────────────────────────────────┐
        │         │ NODE 5: ddl_generator_node                        │
        │         │ ─────────────────────────────────────────────────│
        │         │ Generate based on output_format:                  │
        │         │ - SQL DDL (CREATE TABLE AS SELECT...)             │
        │         │ - PySpark (spark.table().join()...)               │
        │         │ Audience-aware comments and structure             │
        │         └───────────────────────────────────────────────────┘
        │                     │
        │                     ▼
        │         ┌───────────────────────────────────────────────────┐
        │         │ NODE 6: ddl_validation_node                       │
        │         │ ─────────────────────────────────────────────────│
        │         │ Validate:                                         │
        │         │ - All tables exist in ontology                    │
        │         │ - All columns exist in source tables              │
        │         │ - All JOINs match ontology relationships          │
        │         │ - Layer rules enforced                            │
        │         │ - SQL/PySpark syntax valid                        │
        │         └───────────────────────────────────────────────────┘
        │                     │
        │                     ▼
        │         ┌───────────────────────────────────────────────────┐
        │         │ NODE 7: impact_analyzer_node                      │
        │         │ ─────────────────────────────────────────────────│
        │         │ Generate:                                         │
        │         │ - Column details with descriptions                │
        │         │ - Lineage data (for Streamlit visualization)      │
        │         │ - Impact summary (what will be created/changed)   │
        │         │ - For DROP: what depends on this table            │
        │         └───────────────────────────────────────────────────┘
        │                     │
        │                     ▼
        │         ┌───────────────────────────────────────────────────┐
        │         │ HITL 2: code_approval_interrupt                   │
        │         │ ─────────────────────────────────────────────────│
        │         │ Show user:                                        │
        │         │ - Generated DDL/PySpark code                      │
        │         │ - Impact analysis                                 │
        │         │ - Lineage visualization (Streamlit)               │
        │         │ User: Approve Code / Reject with Feedback         │
        │         └───────────────────────────────────────────────────┘
        │                     │
        │         ┌───────────┴───────────┐
        │         │                       │
        │         ▼                       ▼
        │    [Rejected]              [Approved]
        │         │                       │
        └─────────┘                       ▼
    (loop back to              ┌───────────────────────────────────────┐
     NODE 2 with               │ NODE 8: execution_explainer_node      │
     feedback)                 │ ─────────────────────────────────────│
                               │ Explain clearly:                      │
                               │ - What will happen in Databricks      │
                               │ - Tables/columns created or modified  │
                               │ - Relationships affected              │
                               │ - Reversibility (can this be undone?) │
                               └───────────────────────────────────────┘
                                          │
                                          ▼
                               ┌───────────────────────────────────────┐
                               │ HITL 3: execution_approval_interrupt  │
                               │ ─────────────────────────────────────│
                               │ "Are you sure you want to execute?"   │
                               │ Show: Execution summary               │
                               │ User: Execute / Cancel                │
                               └───────────────────────────────────────┘
                                          │
                              ┌───────────┴───────────┐
                              │                       │
                              ▼                       ▼
                         [Cancelled]            [Execute]
                              │                       │
                              ▼                       ▼
                         [Return DDL        ┌───────────────────────────┐
                          without           │ NODE 9: ddl_executor_node │
                          executing]        │ ─────────────────────────│
                              │             │ Execute on Databricks     │
                              │             │ Capture result/errors     │
                              │             └───────────────────────────┘
                              │                        │
                              │            ┌───────────┴───────────┐
                              │            │                       │
                              │            ▼                       ▼
                              │       [Failed]               [Success]
                              │            │                       │
                              │            ▼                       ▼
                              │       [Show Error]    ┌─────────────────────────┐
                              │            │          │ NODE 10: ontology_sync  │
                              │            │          │ ───────────────────────│
                              │            │          │ Update Neo4j:           │
                              │            │          │ - Add new Table node    │
                              │            │          │ - Add Column nodes      │
                              │            │          │ - Add relationships     │
                              │            │          │ - Generate embeddings   │
                              │            │          └─────────────────────────┘
                              │            │                      │
                              │            │                      ▼
                              │            │          ┌─────────────────────────┐
                              │            │          │ NODE 11: ontology_valid │
                              │            │          │ ───────────────────────│
                              │            │          │ Validate:               │
                              │            │          │ - Graph integrity       │
                              │            │          │ - No orphan nodes       │
                              │            │          │ - Embeddings generated  │
                              │            │          │ - Relationships valid   │
                              │            │          └─────────────────────────┘
                              │            │                      │
                              ▼            ▼                      ▼
                         ┌─────────────────────────────────────────────┐
                         │ NODE 12: format_ddl_result_node             │
                         │ ───────────────────────────────────────────│
                         │ Final response:                             │
                         │ - Execution summary (or cancellation msg)   │
                         │ - Ontology update status                    │
                         │ - New table info                            │
                         └─────────────────────────────────────────────┘
                                          │
                                          ▼
                                        [END]
```

---

## 7. Scoring System (v3.0 - Two-Level Scoring)

> **Updated in v3.0**: Scoring now happens at two levels - table discovery and path scoring.
> See [MULTI_SIGNAL_TABLE_DISCOVERY.md](./MULTI_SIGNAL_TABLE_DISCOVERY.md) for complete details.

### 7.1 Level 1: Multi-Signal Table Discovery

Used to find and classify candidate tables before path discovery.

| Signal | Weight | Description |
|--------|--------|-------------|
| Column Semantic Search | 35% | Search `column_descriptions` vector index for column concepts |
| Table Semantic Search | 30% | Search `table_descriptions` vector index for table concepts |
| IS_SOURCE_OF Lineage | 35% | Tables that historically feed the target layer |

**Classification Thresholds:**

| Score Range | Classification | Meaning |
|-------------|----------------|---------|
| ≥ 0.7 | ANCHOR | Must include in final combination |
| 0.2 - 0.7 | CANDIDATE | Include only if on valid path between anchors |
| < 0.2 | EXCLUDE | Do not consider |

### 7.2 Level 2: Path Scoring

After graph pathfinding discovers valid paths between ANCHOR tables, score each path:

| Criterion | Weight | Description |
|-----------|--------|-------------|
| Column Coverage | 30% | Do requested columns exist in path tables? |
| Layer Priority | 25% | Prefer higher maturity data (Gold > Silver > Bronze) |
| Simplicity | 25% | Fewer joins = simpler, better |
| Pattern Match | 20% | Does IS_SOURCE_OF exist for this combo to target layer? |

### Layer Scores (unchanged)

| Layer | Score |
|-------|-------|
| Gold | 100 |
| Silver | 75 |
| Bronze | 50 |

### Simplicity Scoring (replaces Completeness)

| Path Length (joins) | Score |
|---------------------|-------|
| 1 join | 100 |
| 2 joins | 80 |
| 3 joins | 60 |
| 4+ joins | 40 (minimum) |

### Column Coverage Scoring (NEW)

| Coverage | Score |
|----------|-------|
| 100% columns found | 100 |
| 75-99% columns found | 75 |
| 50-74% columns found | 50 |
| < 50% columns found | 25 |

### Pattern Match Scoring

| Condition | Score |
|-----------|-------|
| All source tables have existing IS_SOURCE_OF to target layer | 100 |
| Some source tables have existing IS_SOURCE_OF | 60 |
| No existing lineage patterns found | 30 |

### Example Scoring (v3.0 - Hierarchical Per-Concept)

**Query**: "Create Gold table with PO line items, material group, description, and deletion flag"

**Step 1 - Hierarchical Concept Extraction:**
```json
{
  "table_concepts": [
    {
      "concept_name": "Purchase Order Line Item",
      "description": "PO line item details",
      "column_concepts": [
        {"name": "line item number", "confidence": "high"},
        {"name": "price", "confidence": "high"}
      ]
    },
    {
      "concept_name": "Material Details",
      "description": "Material master attributes",
      "column_concepts": [
        {"name": "material group", "confidence": "high"},
        {"name": "deletion flag", "confidence": "high"}
      ]
    }
  ],
  "shared_columns": [
    {"name": "description", "reason": "ambiguous - could be PO or material"}
  ]
}
```

**Step 2 - Per-Concept Multi-Signal Discovery:**
```
FOR "Purchase Order Line Item":
  Column signal (line item number, price): {ekpo: 0.88, ekko: 0.45}
  Table signal: {ekpo: 0.85, eket: 0.60}
  Lineage signal: {ekpo: 0.90}
  Fused: {ekpo: 0.87, ekko: 0.35, eket: 0.30}
  → ANCHOR: ekpo (score: 0.87)

FOR "Material Details":
  Column signal (material group, deletion flag): {mara: 0.91, makt: 0.65}
  Table signal: {mara: 0.82, marc: 0.55}
  Lineage signal: {mara: 0.85}
  Fused: {mara: 0.86, makt: 0.45, marc: 0.35}
  → ANCHOR: mara (score: 0.86)

concept_anchors = {
  "Purchase Order Line Item": {"anchor": "ekpo", "score": 0.87},
  "Material Details": {"anchor": "mara", "score": 0.86}
}
```

**Step 3 - Shared Column Resolution:**
```
"description" searched across anchors [ekpo, mara]
  → Found in mara.MAKTX (material description) score: 0.78

shared_column_mapping = {"description": {"table": "mara", "column": "MAKTX"}}
```

**Step 4 - Graph Path Discovery:**
```
Anchors to connect: [ekpo, mara]
Query: MATCH path = (a)-[:RELATED_TO|JOINED_ON|...]*1..3]-(b)

discovered_paths = [
  {tables: [ekpo, mara], joins: [{from: ekpo, to: mara, on: MATNR}]},
  {tables: [ekpo, mara, makt], joins: [...]}
]
```

**Step 5 - Path Scoring:**
```
Path 1: ekpo ─[MATNR]─► mara (1 join)
- Column Coverage: line item ✓, price ✓, material group ✓, deletion flag ✓, description ✓ = 100 × 0.30 = 30.0
- Layer Priority: (50 + 50) / 2 = 50 × 0.25 = 12.5
- Simplicity: 1 join = 100 × 0.25 = 25.0
- Pattern Match: Both feed gold = 100 × 0.20 = 20.0
- Total: 87.5/100

Path 2: ekpo ─[MATNR]─► mara ─[MATNR]─► makt (2 joins)
- Column Coverage: all + extra = 100 × 0.30 = 30.0
- Layer Priority: (50 + 50 + 50) / 3 = 50 × 0.25 = 12.5
- Simplicity: 2 joins = 80 × 0.25 = 20.0
- Pattern Match: All feed gold = 100 × 0.20 = 20.0
- Total: 82.5/100
```

**Result**: Path 1 recommended (simpler, covers all concepts and columns)

**Key Benefits of Hierarchical Approach:**
- ✅ Both concepts guaranteed coverage (ekpo for PO, mara for Material)
- ✅ "description" correctly mapped to mara.MAKTX
- ✅ Each concept's columns only boost their concept's anchor

---

## 8. State Schema (v3.0 Updated)

> **Updated in v3.0**: New fields added for Multi-Signal Table Discovery.

Add to `agent/state.py`:

```python
from typing import TypedDict, Optional, List, Dict, Literal

class AgentState(TypedDict):
    # ... existing fields ...

    # ============================================
    # DDL Generation State (v3.0 Updated)
    # ============================================

    # Intent Parsing
    ddl_action: Optional[Literal["CREATE", "ALTER_ADD_COLUMN", "ALTER_DROP_COLUMN", "DROP"]]
    """DDL action type"""

    target_layer: Optional[Literal["silver", "gold"]]
    """Target table layer"""

    target_table_name: Optional[str]
    """Target table full name (optional, can be generated)"""

    output_format: Optional[Literal["sql", "pyspark"]]
    """Output format preference"""

    audience: Optional[Literal["business_user", "data_engineer"]]
    """Target audience for code style"""

    # ============================================
    # Hierarchical Concept Extraction (v3.0 - v2.0 structure)
    # ============================================

    extracted_concepts: Optional[Dict]
    """
    Hierarchical concept extraction from LLM (v2.0 structure):
    {
      'table_concepts': [
        {
          'concept_name': 'Purchase Order Line Item',
          'description': 'PO line item details',
          'column_concepts': [
            {'name': 'line item number', 'confidence': 'high'},
            {'name': 'price', 'confidence': 'high'}
          ]
        },
        {
          'concept_name': 'Material Details',
          'description': 'Material master attributes',
          'column_concepts': [
            {'name': 'material group', 'confidence': 'high'},
            {'name': 'deletion flag', 'confidence': 'high'}
          ]
        }
      ],
      'shared_columns': [
        {'name': 'description', 'reason': 'ambiguous - could be PO or material'}
      ],
      'target_layer': 'gold',
      'action': 'CREATE'
    }

    Key changes from v1.0:
    - column_concepts are NESTED under their parent table_concept
    - shared_columns bucket for ambiguous columns
    - Each column has a confidence score
    """

    requested_concepts: Optional[List[str]]
    """DEPRECATED in v3.0 - Use extracted_concepts.table_concepts instead.
    Kept for backward compatibility."""

    # ============================================
    # Per-Concept Discovery Results (v3.0 NEW)
    # ============================================

    concept_anchors: Optional[Dict]
    """
    Per-concept anchor tables (one anchor per concept):
    {
      'Purchase Order Line Item': {
        'anchor': 'ekpo',
        'score': 0.87,
        'all_candidates': {'ekpo': 0.87, 'ekko': 0.35},
        'column_concepts': ['line item number', 'price']
      },
      'Material Details': {
        'anchor': 'mara',
        'score': 0.86,
        'all_candidates': {'mara': 0.86, 'makt': 0.45},
        'column_concepts': ['material group', 'deletion flag']
      }
    }
    """

    shared_column_mapping: Optional[Dict]
    """
    Mapping of shared columns to tables where found:
    {
      'description': {'table': 'mara', 'column': 'MAKTX'}
    }
    """

    signal_scores: Optional[Dict[str, float]]
    """DEPRECATED - Per-concept scores now in concept_anchors.
    Global fused scores per table (for backward compatibility only)."""

    table_classification: Optional[Dict]
    """
    Concept-based classification (v2.0):
    {
      'anchors': [
        {'table': 'ekpo', 'score': 0.87, 'for_concept': 'Purchase Order Line Item'},
        {'table': 'mara', 'score': 0.86, 'for_concept': 'Material Details'}
      ],
      'candidates': [
        {'table': 'ekko', 'score': 0.35, 'related_concept': 'Purchase Order Line Item'}
      ],
      'anchor_tables': ['ekpo', 'mara']
    }
    """

    discovered_paths: Optional[List[Dict]]
    """
    Valid paths from graph traversal connecting concept anchors:
    [
      {
        'tables': ['ekpo', 'mara'],
        'full_names': ['main...ekpo', 'main...mara'],
        'joins': [{'from': 'ekpo', 'to': 'mara', 'on': 'MATNR'}],
        'path_length': 2,
        'concepts_covered': ['Purchase Order Line Item', 'Material Details']
      }
    ]
    """

    # ============================================
    # Discovery Warnings & Errors (v3.0 NEW)
    # ============================================

    discovery_warnings: Optional[List[Dict]]
    """
    Non-fatal warnings from multi-signal discovery (workflow continues):
    [
      {
        'type': 'CONCEPT_NOT_FOUND',
        'concept': 'Inventory Details',
        'message': 'No matching tables found...',
        'columns_affected': ['stock level']
      },
      {
        'type': 'DUPLICATE_ANCHOR',
        'anchor': 'ekpo',
        'concepts': ['PO Header', 'PO Line'],
        'message': 'Table ekpo is anchor for multiple concepts'
      },
      {
        'type': 'MISSING_COLUMNS',
        'path': ['ekpo', 'mara'],
        'missing': ['unit price'],
        'message': 'Some columns not found in path'
      }
    ]
    """

    discovery_error: Optional[Dict]
    """
    Fatal error from multi-signal discovery (stops workflow, routes to format_ddl_result):
    {
      'error': 'NO_ANCHORS_FOUND',  # or INVALID_TARGET_LAYER, NO_PATHS_FOUND, NO_CONCEPTS_OR_COLUMNS
      'message': 'Human-readable explanation',
      'suggestion': 'What user can do to fix it'
    }
    """

    needs_clarification: Optional[Dict]
    """
    Clarification request from discovery (triggers concept_clarification_gate HITL):
    {
      'needs_clarification': True,
      'clarification_type': 'CONCEPT_AMBIGUOUS',
      'question': 'Which business area do these columns relate to?',
      'options': ['Purchase Orders', 'Sales Orders', 'Materials', 'Vendors', 'Customers', 'Other']
    }
    """

    user_clarification_response: Optional[str]
    """User's response to clarification question (selected option or custom text)"""

    # Source Discovery (UPDATED - now populated by path discovery)
    all_source_combinations: Optional[List[Dict]]
    """DEPRECATED - Use discovered_paths instead.
    Valid paths discovered through graph traversal (not Cartesian product)."""

    scored_combinations: Optional[List[Dict]]
    """Top 3 scored paths with explanations"""

    formatted_recommendations: Optional[List[Dict]]
    """Formatted recommendations for UI display"""

    # User Selection
    selected_option_index: Optional[int]
    """User's selected option (0, 1, or 2)"""

    user_feedback: Optional[str]
    """User feedback for rejection or modification"""

    feedback_iteration: int
    """Track feedback iterations (max 5)"""

    # Generation
    generated_ddl: Optional[str]
    """Generated DDL or PySpark code"""

    ddl_validation_result: Optional[Dict]
    """Validation results"""

    code_approved: Optional[bool]
    """User approved the generated code (HITL 2)"""

    # Impact Analysis
    impact_analysis: Optional[Dict]
    """Detailed impact analysis"""

    lineage_data: Optional[Dict]
    """Lineage data for Streamlit visualization"""

    column_explanations: Optional[List[Dict]]
    """Column details with descriptions"""

    # Execution
    execution_explanation: Optional[str]
    """Human-readable explanation of what will happen"""

    execution_approved: Optional[bool]
    """User approved execution"""

    execution_result: Optional[Dict]
    """Result from Databricks execution"""

    # Ontology Sync
    ontology_sync_result: Optional[Dict]
    """Result of Neo4j ontology update"""

    ontology_validation_result: Optional[Dict]
    """Result of ontology validation after sync"""

    # Final Output
    final_response: Optional[str]
    """Final formatted response to show user (from format_ddl_result_node)"""
```

---

## 9. Node Specifications

### NODE 1: ddl_intent_parser_node (v3.0 - Hierarchical Concept Extraction)

> **Updated in v3.0**: Now uses **two-pass hierarchical extraction** where column_concepts are
> nested under their parent table_concepts. Based on RSL-SQL, MAC-SQL, AmbiSQL research (2024-2025).

```python
class TableConcept(BaseModel):
    """A business entity with its related column concepts."""
    concept_name: str           # e.g., "Purchase Order Line Item"
    description: str            # e.g., "PO line item details"
    column_concepts: List[Dict] # e.g., [{"name": "price", "confidence": "high"}]


class SharedColumn(BaseModel):
    """An ambiguous column with no clear parent concept."""
    name: str                   # e.g., "description"
    reason: str                 # e.g., "ambiguous - could be PO or material"


class DDLIntent(BaseModel):
    """Structured output for DDL intent parsing (v3.0 - Hierarchical)"""
    action: Literal["CREATE", "ALTER_ADD_COLUMN", "ALTER_DROP_COLUMN", "DROP"]
    target_layer: Literal["silver", "gold"]
    target_table_name: Optional[str] = None
    output_format: Literal["sql", "pyspark"] = "sql"
    audience: Literal["business_user", "data_engineer"] = "data_engineer"

    # v3.0: Hierarchical concept extraction
    table_concepts: List[TableConcept]  # Business entities with nested columns
    shared_columns: List[SharedColumn]  # Ambiguous columns (no clear parent)

    reasoning: str


def ddl_intent_parser_node(state: AgentState) -> Dict:
    """
    Parse user's DDL request with hierarchical concept extraction (v3.0).

    Two-pass extraction:
    PASS 1: Extract table_concepts (business entities)
    PASS 2: For each table_concept, extract child column_concepts
    PASS 3: Identify shared_columns (ambiguous, no clear parent)
    """

    llm = get_llm().with_structured_output(DDLIntent)

    prompt = f"""Analyze this DDL/PySpark generation request:

User Query: {state["user_query"]}

Extract:
1. Action: What DDL operation? (CREATE new table, ALTER to add/drop column, DROP table)
2. Target Layer: silver or gold?
3. Target Table Name: If user specified a name, extract it. Otherwise null.
4. Output Format: SQL DDL or PySpark? Default to SQL unless user mentions PySpark/Spark/notebook.
5. Audience: Business user (simplified with comments) or Data engineer (full technical)?

**IMPORTANT - Hierarchical Concept Extraction (v3.0):**

6. Table Concepts: Identify BUSINESS ENTITIES/OBJECTS mentioned, then for EACH entity,
   identify which column/field mentions belong to it.

   Structure:
   [
     {{
       "concept_name": "Purchase Order Line Item",
       "description": "Details about PO line items",
       "column_concepts": [
         {{"name": "line item number", "confidence": "high"}},
         {{"name": "price", "confidence": "high"}},
         {{"name": "quantity", "confidence": "high"}}
       ]
     }},
     {{
       "concept_name": "Material Details",
       "description": "Material master attributes",
       "column_concepts": [
         {{"name": "material group", "confidence": "high"}},
         {{"name": "deletion flag", "confidence": "high"}}
       ]
     }}
   ]

7. Shared Columns: Columns that DON'T clearly belong to any single concept.
   Examples: "description" (could be PO description or material description)
   [
     {{"name": "description", "reason": "ambiguous - could be PO or material description"}}
   ]

Rules:
- Silver tables can source from: bronze, silver
- Gold tables can source from: bronze, silver, gold
- Columns should be placed under the concept they MOST LIKELY belong to
- Use "high" confidence when clear, "medium" when somewhat related, "low" when uncertain
- If a column could belong to multiple concepts, put it in shared_columns instead
- If user says "PySpark", "Spark", "notebook", "script" → pyspark format
- If user says "business", "simple", "explain" → business_user audience

Examples of correct categorization:
- "PO line items" → table_concept with columns like "line item number", "price"
- "material details" → table_concept with columns like "material group", "deletion flag"
- "material group" → column under "Material Details" concept
- "deletion flag" → column under "Material Details" concept
- "description" → shared_column if not clear which entity it describes
"""

    result = llm.invoke(prompt)

    # Build extracted_concepts dict with hierarchical structure
    extracted_concepts = {
        "table_concepts": [
            {
                "concept_name": tc.concept_name,
                "description": tc.description,
                "column_concepts": tc.column_concepts
            }
            for tc in result.table_concepts
        ],
        "shared_columns": [
            {"name": sc.name, "reason": sc.reason}
            for sc in result.shared_columns
        ],
        "target_layer": result.target_layer,
        "action": result.action
    }

    return {
        "ddl_action": result.action,
        "target_layer": result.target_layer,
        "target_table_name": result.target_table_name,
        "output_format": result.output_format,
        "audience": result.audience,
        "extracted_concepts": extracted_concepts,
        # Backward compatibility: flatten for old code
        "requested_concepts": [tc.concept_name for tc in result.table_concepts],
        "feedback_iteration": 0
    }
```

### NODE 2: multi_signal_discovery_node (v3.0 - Per-Concept Discovery)

> **MAJOR UPDATE v3.0**: This node now runs signal discovery **PER CONCEPT** instead of globally.
> Each table_concept gets its own anchor table, guaranteeing all user concepts are covered.
> See [MULTI_SIGNAL_TABLE_DISCOVERY.md](./MULTI_SIGNAL_TABLE_DISCOVERY.md) for complete algorithm details.

#### Edge Case Handling (v3.0)

This node includes robust handling for 6 edge cases:

| Edge Case | Strategy | Result |
|-----------|----------|--------|
| No table_concepts extracted | Infer from columns → Ask user | Graceful recovery or clarification |
| Concept with no signal matches | Partial results + warning | Continues with remaining concepts |
| Tie-breaking for identical scores | Secondary sort by path_length | Deterministic selection |
| Invalid target_layer | Validate at entry | Early error with valid options |
| Same table is anchor for multiple concepts | Allow + info message | Normal operation |
| Concept's columns not in anchor | Early warning | Proceeds with available columns |

```python
def multi_signal_discovery_node(state: AgentState) -> Dict:
    """
    v3.0: Per-Concept Multi-Signal Discovery with Graph Pathfinding.

    Key change from v2.0: Signals run FOR EACH concept separately,
    ensuring every concept gets its own anchor table.

    Flow:
    1. Validate inputs (layer, concepts)
    2. FOR EACH table_concept:
       - Run column signal (its child columns only)
       - Run table signal (the concept name)
       - Run lineage signal
       - Fuse and select best table = ANCHOR for this concept
    3. Handle shared_columns across all anchors
    4. Graph pathfinding between concept ANCHORs
    5. Return valid paths covering ALL concepts
    """
    from tools.neo4j_tool import execute_cypher
    from tools.graphrag_retriever import get_embedding

    target_layer = state["target_layer"]
    extracted = state.get("extracted_concepts", {})
    user_query = state.get("user_query", "")
    user_feedback = state.get("user_feedback")

    # ============================================
    # EDGE CASE 4: Validate target_layer
    # ============================================
    valid_layers = ["silver", "gold"]
    if target_layer not in valid_layers:
        return {
            "discovery_error": {
                "error": "INVALID_TARGET_LAYER",
                "message": f"Invalid target_layer: '{target_layer}'",
                "valid_options": valid_layers,
                "suggestion": "Please specify 'silver' or 'gold' as the target layer."
            }
        }

    # ============================================
    # EDGE CASE 1: Handle no table_concepts
    # ============================================
    table_concepts = extracted.get("table_concepts", [])
    shared_columns = extracted.get("shared_columns", [])

    # Check if user provided clarification response (from concept_clarification_gate)
    user_clarification = state.get("user_clarification_response")

    if not table_concepts:
        # If user already provided clarification, use it to construct table_concepts
        if user_clarification:
            table_concepts = [_build_concept_from_clarification(
                user_clarification,
                shared_columns,
                user_query
            )]
            # Clear clarification state to prevent reprocessing
            # Update extracted_concepts with the new structure
            extracted = {
                "table_concepts": table_concepts,
                "shared_columns": [],  # Moved to table_concepts
                "target_layer": target_layer,
                "action": extracted.get("action", "CREATE"),
                "from_clarification": True
            }
        else:
            # No clarification yet - try to infer or ask user
            handled = _handle_no_table_concepts(extracted, user_query)
            if "error" in handled:
                # Hard error - no concepts or columns at all
                return {"discovery_error": handled}
            if "needs_clarification" in handled:
                # Soft error - need user clarification
                return {"needs_clarification": handled}
            # If inference succeeded, use the inferred concepts
            table_concepts = handled.get("table_concepts", [])
            shared_columns = handled.get("shared_columns", [])
            extracted = handled  # Update extracted with inference

    # Determine allowed source layers
    if target_layer == "silver":
        allowed_layers = ["bronze", "silver"]
    else:  # gold
        allowed_layers = ["bronze", "silver", "gold"]

    # ============================================
    # STEP 1: Per-Concept Multi-Signal Discovery
    # ============================================
    concept_anchors = {}
    warnings = []  # Collect warnings for partial success

    for concept in table_concepts:
        concept_name = concept["concept_name"]
        child_columns = [c["name"] for c in concept.get("column_concepts", [])]

        # Signal 1: Column semantic search (35% weight) - ONLY this concept's columns
        column_signal = _search_by_columns(child_columns, allowed_layers)

        # Signal 2: Table semantic search (30% weight) - the concept name
        table_signal = _search_by_tables([concept_name], allowed_layers)

        # Signal 3: IS_SOURCE_OF lineage patterns (35% weight)
        lineage_signal = _search_by_lineage([concept_name], target_layer, allowed_layers)

        # Fuse signals for THIS CONCEPT
        fused = _fuse_signals(column_signal, table_signal, lineage_signal)

        # ============================================
        # EDGE CASE 2: Concept with no signal matches
        # ============================================
        if not fused:
            warnings.append({
                "type": "CONCEPT_NOT_FOUND",
                "concept": concept_name,
                "message": f"No matching tables found for '{concept_name}'. "
                           f"This concept will be skipped.",
                "columns_affected": child_columns
            })
            continue  # Skip this concept, continue with others

        # ============================================
        # EDGE CASE 3: Tie-breaking for identical scores
        # Sort by score DESC, then by path_length ASC (prefer simpler tables)
        # ============================================
        sorted_candidates = sorted(
            fused.items(),
            key=lambda x: (-x[1], _get_table_path_length(x[0]))
        )
        best_table = sorted_candidates[0]

        concept_anchors[concept_name] = {
            "anchor": best_table[0],
            "score": best_table[1],
            "all_candidates": fused,
            "column_concepts": child_columns
        }

    # ============================================
    # EDGE CASE 5: Detect duplicate anchors (same table for multiple concepts)
    # ============================================
    anchor_to_concepts = {}
    for concept_name, result in concept_anchors.items():
        anchor = result["anchor"]
        if anchor not in anchor_to_concepts:
            anchor_to_concepts[anchor] = []
        anchor_to_concepts[anchor].append(concept_name)

    for anchor, concepts in anchor_to_concepts.items():
        if len(concepts) > 1:
            warnings.append({
                "type": "DUPLICATE_ANCHOR",
                "anchor": anchor,
                "concepts": concepts,
                "message": f"Table '{anchor}' is the best match for multiple concepts: {concepts}. "
                           f"This is valid but may indicate concept overlap."
            })

    # ============================================
    # STEP 2: Handle Shared Columns
    # ============================================
    anchor_tables = [ca["anchor"] for ca in concept_anchors.values()]
    shared_column_mapping = _resolve_shared_columns(shared_columns, anchor_tables, allowed_layers)

    # ============================================
    # STEP 3: Collect Anchors (one per concept)
    # ============================================
    anchors = []
    for concept_name, result in concept_anchors.items():
        anchors.append({
            "table": result["anchor"],
            "score": result["score"],
            "for_concept": concept_name,
            "column_concepts": result.get("column_concepts", [])
        })

    if not anchors:
        return {
            "discovery_error": {
                "error": "NO_ANCHORS_FOUND",
                "message": "No anchor tables found for any concept. Please provide more specific requirements.",
                "suggestion": "Try specifying table names like 'purchase orders' or 'materials'."
            },
            "concept_anchors": concept_anchors,
            "discovery_warnings": warnings if warnings else None
        }

    # ============================================
    # STEP 4: Graph Path Discovery
    # ============================================
    anchor_names = [a["table"] for a in anchors]

    discovered_paths = _find_graph_paths(anchor_names, [], max_depth=3)

    if not discovered_paths and len(anchor_names) > 1:
        return {
            "discovery_error": {
                "error": "NO_PATHS_FOUND",
                "message": f"No valid paths found connecting concept anchors: {anchor_names}. Check Neo4j relationships.",
                "suggestion": "The requested concepts may not have direct relationships. Try simpler queries."
            },
            "concept_anchors": concept_anchors,
            "table_classification": {"anchors": anchors, "anchor_tables": anchor_names},
            "discovery_warnings": warnings if warnings else None
        }

    # Handle single anchor case
    if not discovered_paths and len(anchor_names) == 1:
        discovered_paths = [{
            "tables": anchor_names,
            "full_names": [_get_table_full_name(anchor_names[0])],
            "joins": [],
            "path_length": 1
        }]

    # ============================================
    # STEP 5: Enrich paths with column data and concept mapping
    # ============================================
    # Flatten all column concepts from all table concepts
    all_column_concepts = []
    for concept in table_concepts:
        all_column_concepts.extend([c["name"] for c in concept.get("column_concepts", [])])

    for path in discovered_paths:
        path["columns"] = _get_relevant_columns(path["tables"], all_column_concepts)
        path["layers"] = [_get_table_layer(t) for t in path["tables"]]
        path["concepts_covered"] = list(concept_anchors.keys())

        # ============================================
        # EDGE CASE 6: Check for missing columns
        # ============================================
        available_columns = set(col["name"] for col in path["columns"])
        requested_columns = set(all_column_concepts)
        missing_columns = requested_columns - available_columns

        if missing_columns:
            warnings.append({
                "type": "MISSING_COLUMNS",
                "path": path["tables"],
                "missing": list(missing_columns),
                "message": f"Some requested columns not found in path: {list(missing_columns)}"
            })

    return {
        "extracted_concepts": extracted,
        "concept_anchors": concept_anchors,
        "shared_column_mapping": shared_column_mapping,
        "table_classification": {
            "anchors": anchors,
            "anchor_tables": anchor_names
        },
        "discovered_paths": discovered_paths,
        "all_source_combinations": discovered_paths,  # Backward compatibility
        "discovery_warnings": warnings if warnings else None,  # Non-fatal warnings
        "discovery_error": None,  # No error - success case
        "needs_clarification": None,  # Clear any previous clarification request
        "user_clarification_response": None  # Clear after successful use
    }


def _handle_no_table_concepts(extracted_concepts: Dict, user_query: str) -> Dict:
    """
    Edge Case 1: Handle case where LLM extracted no table_concepts.
    Strategy: Try to infer from columns, then ask user if still ambiguous.
    """
    shared_columns = extracted_concepts.get("shared_columns", [])

    if not shared_columns:
        return {
            "error": "NO_CONCEPTS_OR_COLUMNS",
            "message": "Could not identify any business concepts or columns from query.",
            "suggestion": "Please specify what data you need (e.g., 'purchase orders', 'materials', 'vendors')."
        }

    # Try to infer table_concept from columns
    inferred = _infer_concept_from_columns(shared_columns, user_query)

    if inferred["confidence"] >= 0.7:
        return {
            "table_concepts": [{
                "concept_name": inferred["concept_name"],
                "description": f"Inferred from columns: {[c['name'] for c in shared_columns]}",
                "column_concepts": [{"name": c["name"], "confidence": "inferred"} for c in shared_columns]
            }],
            "shared_columns": [],
            "inferred": True
        }
    else:
        return {
            "needs_clarification": True,
            "clarification_type": "CONCEPT_AMBIGUOUS",
            "question": f"I found these columns but couldn't determine the business area: {[c['name'] for c in shared_columns]}. Which area do these relate to?",
            "options": ["Purchase Orders", "Sales Orders", "Materials", "Vendors", "Customers", "Other"]
        }


def _infer_concept_from_columns(columns: List[Dict], user_query: str) -> Dict:
    """
    Attempt to infer the table concept from column names.
    Uses keyword matching and query context.
    """
    column_names = [c["name"].lower() for c in columns]
    query_lower = user_query.lower()

    # Keyword mappings for common concepts
    concept_keywords = {
        "Purchase Order Header": ["ebeln", "po number", "purchase order", "purchasing doc"],
        "Purchase Order Line Item": ["ebelp", "po item", "line item", "po line"],
        "Material Master": ["matnr", "material", "material number", "mara"],
        "Vendor Master": ["lifnr", "vendor", "supplier", "lfa1"],
        "Customer Master": ["kunnr", "customer", "sold-to", "kna1"],
        "Sales Order Header": ["vbeln", "sales order", "sales doc", "vbak"],
        "Sales Order Item": ["posnr", "sales item", "vbap"]
    }

    best_match = {"concept_name": None, "confidence": 0.0}

    for concept, keywords in concept_keywords.items():
        score = 0.0
        for kw in keywords:
            # Check in column names
            if any(kw in col for col in column_names):
                score += 0.4
            # Check in user query
            if kw in query_lower:
                score += 0.3

        if score > best_match["confidence"]:
            best_match = {"concept_name": concept, "confidence": min(score, 1.0)}

    return best_match


def _get_table_path_length(table_name: str) -> int:
    """
    Get a proxy for table complexity/path_length for tie-breaking.
    Tables in bronze layer get lower values (preferred).
    """
    from tools.neo4j_tool import execute_cypher

    result = execute_cypher("""
        MATCH (t:Table {name: $table_name})
        RETURN t.layer AS layer
    """, {"table_name": table_name})

    if result:
        layer = result[0].get("layer", "gold")
        # Lower is better for tie-breaking (prefer simpler/source tables)
        layer_scores = {"bronze": 1, "silver": 2, "gold": 3}
        return layer_scores.get(layer, 2)
    return 2  # Default to silver


def _build_concept_from_clarification(
    user_response: str,
    shared_columns: List[Dict],
    user_query: str
) -> Dict:
    """
    Build a table_concept from user's clarification response.
    Called when user answers the concept_clarification_gate.

    Args:
        user_response: User's selected option (e.g., "Purchase Orders")
        shared_columns: The columns that were found but couldn't be assigned
        user_query: Original user query for context

    Returns:
        A table_concept dict with the user's specified concept
    """
    # Map user-friendly names to formal concept names
    concept_mapping = {
        "Purchase Orders": "Purchase Order Header",
        "Sales Orders": "Sales Order Header",
        "Materials": "Material Master",
        "Vendors": "Vendor Master",
        "Customers": "Customer Master",
    }

    concept_name = concept_mapping.get(user_response, user_response)

    return {
        "concept_name": concept_name,
        "description": f"User-specified: {user_response}",
        "column_concepts": [
            {"name": col["name"], "confidence": "user_confirmed"}
            for col in shared_columns
        ],
        "from_clarification": True
    }


def _resolve_shared_columns(
    shared_columns: List[Dict],
    anchor_tables: List[str],
    allowed_layers: List[str]
) -> Dict:
    """Search shared columns across all concept anchors."""
    from tools.neo4j_tool import execute_cypher
    from tools.graphrag_retriever import get_embedding

    results = {}
    for col in shared_columns:
        col_name = col["name"]
        embedding = get_embedding(col_name)

        # Search in anchor tables only
        matches = execute_cypher("""
            CALL db.index.vector.queryNodes('column_descriptions', 3, $embedding)
            YIELD node AS col, score
            MATCH (t:Table)-[:HAS_COLUMN]->(col)
            WHERE t.name IN $anchor_tables AND score > 0.6
            RETURN t.name AS table_name, col.name AS column_name, score
            ORDER BY score DESC
            LIMIT 1
        """, {"embedding": embedding, "anchor_tables": anchor_tables})

        if matches:
            results[col_name] = {
                "table": matches[0]["table_name"],
                "column": matches[0]["column_name"],
                "score": matches[0]["score"]
            }
        else:
            results[col_name] = {"table": None, "column": None, "reason": "not found in anchor tables"}

    return results


def _search_by_columns(column_concepts: List[str], allowed_layers: List[str]) -> Dict[str, float]:
    """Signal 1: Search column embeddings, return parent tables with scores."""
    from tools.neo4j_tool import execute_cypher
    from tools.graphrag_retriever import get_embedding

    results = {}
    for concept in column_concepts:
        embedding = get_embedding(concept)

        matches = execute_cypher("""
            CALL db.index.vector.queryNodes('column_descriptions', 5, $embedding)
            YIELD node AS col, score
            MATCH (t:Table)-[:HAS_COLUMN]->(col)
            WHERE score > 0.7 AND t.layer IN $allowed_layers
            RETURN t.name AS table_name, t.full_name AS full_name, score
            ORDER BY score DESC
        """, {"embedding": embedding, "allowed_layers": allowed_layers})

        for match in matches:
            table = match["table_name"]
            score = match["score"]
            results[table] = max(results.get(table, 0), score)

    return results


def _search_by_tables(table_concepts: List[str], allowed_layers: List[str]) -> Dict[str, float]:
    """Signal 2: Search table embeddings directly."""
    from tools.neo4j_tool import execute_cypher
    from tools.graphrag_retriever import get_embedding

    results = {}
    for concept in table_concepts:
        embedding = get_embedding(concept)

        matches = execute_cypher("""
            CALL db.index.vector.queryNodes('table_descriptions', 5, $embedding)
            YIELD node AS t, score
            WHERE score > 0.6 AND t.layer IN $allowed_layers
            RETURN t.name AS table_name, t.full_name AS full_name, score
            ORDER BY score DESC
        """, {"embedding": embedding, "allowed_layers": allowed_layers})

        for match in matches:
            table = match["table_name"]
            score = match["score"]
            results[table] = max(results.get(table, 0), score)

    return results


def _search_by_lineage(concepts: List[str], target_layer: str, allowed_layers: List[str]) -> Dict[str, float]:
    """Signal 3: Find tables that historically feed the target layer."""
    from tools.neo4j_tool import execute_cypher

    matches = execute_cypher("""
        MATCH (dst:Table {layer: $target_layer})
        WHERE any(concept IN $concepts WHERE
            toLower(dst.description) CONTAINS toLower(concept)
            OR toLower(dst.name) CONTAINS toLower(concept))
        MATCH (src:Table)-[:IS_SOURCE_OF*1..2]->(dst)
        WHERE src.layer IN $allowed_layers
        RETURN src.name AS table_name, count(DISTINCT dst) AS feeds_count
        ORDER BY feeds_count DESC
        LIMIT 10
    """, {"target_layer": target_layer, "concepts": concepts, "allowed_layers": allowed_layers})

    if not matches:
        return {}

    max_count = max(m["feeds_count"] for m in matches)
    return {m["table_name"]: m["feeds_count"] / max_count for m in matches}


def _fuse_signals(column_signal: Dict, table_signal: Dict, lineage_signal: Dict) -> Dict[str, float]:
    """Weighted combination of all signals."""
    WEIGHTS = {"column": 0.35, "table": 0.30, "lineage": 0.35}

    all_tables = set(column_signal.keys()) | set(table_signal.keys()) | set(lineage_signal.keys())

    combined = {}
    for table in all_tables:
        score = (
            column_signal.get(table, 0) * WEIGHTS["column"] +
            table_signal.get(table, 0) * WEIGHTS["table"] +
            lineage_signal.get(table, 0) * WEIGHTS["lineage"]
        )
        combined[table] = round(score, 3)

    return dict(sorted(combined.items(), key=lambda x: -x[1]))


def _classify_tables(scores: Dict[str, float], anchor_threshold: float = 0.7) -> Dict:
    """Classify tables into ANCHOR, CANDIDATE, EXCLUDE."""
    CANDIDATE_THRESHOLD = 0.2

    anchors, candidates, excluded = [], [], []

    for table, score in scores.items():
        entry = {"table": table, "score": score}
        if score >= anchor_threshold:
            anchors.append(entry)
        elif score >= CANDIDATE_THRESHOLD:
            candidates.append(entry)
        else:
            excluded.append(entry)

    return {"anchors": anchors, "candidates": candidates, "excluded": excluded}


def _find_graph_paths(anchors: List[str], candidates: List[str], max_depth: int = 3) -> List[Dict]:
    """Find valid paths between anchor tables via Neo4j traversal."""
    from tools.neo4j_tool import execute_cypher

    if len(anchors) < 2:
        # Single anchor - return it directly
        return [{"tables": anchors, "joins": [], "path_length": 1}]

    paths = execute_cypher("""
        // Use all join relationship types available in Neo4j
        MATCH path = (start:Table)-[:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER*1..3]-(end:Table)
        WHERE start.name IN $anchors AND end.name IN $anchors AND start <> end
        WITH path,
             [node IN nodes(path) | node.name] AS table_names,
             [node IN nodes(path) | node.full_name] AS full_names,
             [rel IN relationships(path) | {
               from_table: startNode(rel).name,
               to_table: endNode(rel).name,
               join_column: rel.src_column,
               join_condition: rel.join_condition
             }] AS joins
        RETURN DISTINCT table_names AS tables, full_names, joins, size(table_names) AS path_length
        ORDER BY path_length ASC
        LIMIT 20
    """, {"anchors": anchors})

    return [
        {
            "tables": p["tables"],
            "full_names": p["full_names"],
            "joins": p["joins"],
            "path_length": p["path_length"]
        }
        for p in paths
    ]


def _get_table_layer(table_name: str) -> str:
    """Get layer for a table from Neo4j."""
    from tools.neo4j_tool import execute_cypher
    result = execute_cypher("MATCH (t:Table {name: $name}) RETURN t.layer AS layer", {"name": table_name})
    return result[0]["layer"] if result else "unknown"
```

> **DEPRECATED**: The old `_generate_table_combinations()` function using Cartesian product is no longer used.
> Graph pathfinding via `_find_graph_paths()` replaces it.

### NODE 3: path_scorer_node (v3.0 - Replaces combination_scorer)

> **Updated in v3.0**: Renamed to `path_scorer_node`. Now scores discovered paths instead of combinations.
> Adds Column Coverage scoring and updated weights.

```python
class PathScorer:
    """Score discovered paths for DDL generation (v3.0)."""

    # v3.0 Updated weights
    WEIGHTS = {
        "column_coverage": 0.30,   # NEW: Do requested columns exist?
        "layer_priority": 0.25,    # Reduced from 0.40
        "simplicity": 0.25,        # Renamed from completeness
        "pattern_match": 0.20      # Reduced from 0.15
    }
    LAYER_SCORES = {"gold": 100, "silver": 75, "bronze": 50}

    def score(self, path: Dict, target_layer: str, column_concepts: List[str]) -> Dict:
        column_coverage = self._calc_column_coverage(path, column_concepts)
        layer_score = self._calc_layer_priority(path)
        simplicity_score = self._calc_simplicity(path)
        pattern_score = self._calc_pattern_match(path, target_layer)

        total = (
            column_coverage["score"] * self.WEIGHTS["column_coverage"] +
            layer_score * self.WEIGHTS["layer_priority"] +
            simplicity_score * self.WEIGHTS["simplicity"] +
            pattern_score * self.WEIGHTS["pattern_match"]
        )

        return {
            "path": path,
            "total_score": round(total, 1),
            "breakdown": {
                "column_coverage": {"score": column_coverage["score"], "weight": "30%",
                                   "covered": column_coverage["covered"],
                                   "missing": column_coverage["missing"]},
                "layer_priority": {"score": layer_score, "weight": "25%"},
                "simplicity": {"score": simplicity_score, "weight": "25%"},
                "pattern_match": {"score": pattern_score, "weight": "20%"}
            },
            "explanation": self._generate_explanation(
                path, column_coverage, layer_score, simplicity_score, pattern_score
            )
        }

    def _calc_column_coverage(self, path: Dict, column_concepts: List[str]) -> Dict:
        """NEW: Check what percentage of requested columns exist in path tables."""
        if not column_concepts:
            return {"score": 100, "covered": [], "missing": []}

        columns = path.get("columns", [])
        column_texts = [f"{c.get('name', '')} {c.get('description', '')}".lower() for c in columns]

        covered, missing = [], []
        for concept in column_concepts:
            found = any(concept.lower() in col_text for col_text in column_texts)
            if found:
                covered.append(concept)
            else:
                missing.append(concept)

        coverage_ratio = len(covered) / len(column_concepts) if column_concepts else 1.0

        if coverage_ratio >= 1.0:
            score = 100
        elif coverage_ratio >= 0.75:
            score = 75
        elif coverage_ratio >= 0.5:
            score = 50
        else:
            score = 25

        return {"score": score, "covered": covered, "missing": missing}

    def _calc_layer_priority(self, path: Dict) -> float:
        layers = path.get("layers", [])
        if not layers:
            return 50
        return sum(self.LAYER_SCORES.get(l, 50) for l in layers) / len(layers)

    def _calc_simplicity(self, path: Dict) -> float:
        """Score based on path length (fewer joins = better)."""
        path_length = path.get("path_length", len(path.get("tables", [])))
        if path_length <= 2:
            return 100
        elif path_length == 3:
            return 80
        elif path_length == 4:
            return 60
        else:
            return 40

    def _calc_pattern_match(self, path: Dict, target_layer: str) -> float:
        """Check if path tables have IS_SOURCE_OF to target layer."""
        from tools.neo4j_tool import execute_cypher

        tables = path.get("tables", [])
        if not tables:
            return 30

        result = execute_cypher("""
            MATCH (src:Table)-[:IS_SOURCE_OF]->(dst:Table {layer: $target_layer})
            WHERE src.name IN $tables
            RETURN count(DISTINCT src) AS matched_count
        """, {"tables": tables, "target_layer": target_layer})

        matched = result[0]["matched_count"] if result else 0
        ratio = matched / len(tables)

        if ratio >= 1.0:
            return 100
        elif ratio >= 0.5:
            return 60
        else:
            return 30

    def _generate_explanation(self, path, column_coverage, layer_score,
                              simplicity_score, pattern_score) -> str:
        """Generate human-readable explanation."""
        explanations = []

        # Column coverage
        if column_coverage["score"] >= 100:
            explanations.append("All requested columns found")
        elif column_coverage["missing"]:
            explanations.append(f"Missing columns: {', '.join(column_coverage['missing'])}")

        # Layer priority
        if layer_score >= 75:
            explanations.append("Uses high-quality source data (Silver/Gold)")
        elif layer_score >= 50:
            explanations.append("Uses Bronze source data")

        # Simplicity
        if simplicity_score >= 80:
            explanations.append("Simple structure (few joins)")
        else:
            explanations.append("Complex structure (multiple joins)")

        # Pattern match
        if pattern_score >= 80:
            explanations.append("Follows established lineage patterns")
        elif pattern_score < 50:
            explanations.append("Novel combination")

        return "; ".join(explanations)


def combination_scorer_node(state: AgentState) -> Dict:
    """Score all discovered paths and return top 3."""
    scorer = PathScorer()

    # Get column concepts for coverage scoring (v3.0 hierarchical structure)
    extracted = state.get("extracted_concepts", {})
    # Flatten column_concepts from hierarchical table_concepts
    column_concepts = []
    for tc in extracted.get("table_concepts", []):
        column_concepts.extend([c["name"] for c in tc.get("column_concepts", [])])
    # Also include shared_columns
    column_concepts.extend([c["name"] for c in extracted.get("shared_columns", [])])

    # Score paths (from discovered_paths or all_source_combinations for backward compat)
    paths = state.get("discovered_paths") or state.get("all_source_combinations", [])

    scored = [
        scorer.score(p, state["target_layer"], column_concepts)
        for p in paths
    ]
    scored.sort(key=lambda x: x["total_score"], reverse=True)

    return {"scored_combinations": scored[:3]}
```

### NODE 4: recommendation_presenter_node

```python
def recommendation_presenter_node(state: AgentState) -> Dict:
    """
    Format scored combinations into user-friendly recommendations for HITL display.

    Transforms raw scored data into a presentation format with:
    - Option letters (A, B, C)
    - Score breakdown with explanations
    - Table aliases (T1, T2, T3...)
    - JOIN conditions in readable format
    - Column preview with descriptions
    - Lineage preview showing data flow
    """
    scored_combinations = state["scored_combinations"]
    target_layer = state["target_layer"]
    option_letters = ["A", "B", "C"]

    formatted_recommendations = []

    for idx, scored in enumerate(scored_combinations):
        combo = scored["combination"]
        tables = combo["tables"]

        # Assign table aliases (T1, T2, T3...)
        table_aliases = {}
        for i, table in enumerate(tables):
            alias = f"T{i + 1}"
            table_aliases[table["full_name"]] = alias
            table["alias"] = alias

        # Format JOIN conditions
        formatted_joins = []
        for join_path in combo.get("join_paths", []):
            src_alias = table_aliases.get(join_path["source_table"], "?")
            dst_alias = table_aliases.get(join_path["target_table"], "?")
            formatted_joins.append({
                "source": f"{src_alias}.{join_path['source_column']}",
                "target": f"{dst_alias}.{join_path['target_column']}",
                "condition": f"{src_alias}.{join_path['source_column']} = {dst_alias}.{join_path['target_column']}",
                "join_type": join_path.get("join_type", "INNER")
            })

        # Format column preview (first 10 columns)
        formatted_columns = []
        for col in combo.get("columns", [])[:10]:
            src_alias = table_aliases.get(col.get("source_table", ""), "?")
            formatted_columns.append({
                "name": col.get("alias", col["name"]),
                "source": f"{src_alias}.{col['name']}",
                "data_type": col.get("data_type", "STRING"),
                "description": col.get("description", "")[:100]  # Truncate long descriptions
            })

        # Create lineage preview (source tables → target)
        lineage_preview = {
            "sources": [
                {"name": t["name"], "layer": t["layer"], "alias": t["alias"]}
                for t in tables
            ],
            "target_layer": target_layer,
            "flow": " + ".join([t["alias"] for t in tables]) + f" → {target_layer.upper()} table"
        }

        # Build recommendation object
        recommendation = {
            "option_letter": option_letters[idx],
            "option_index": idx,
            "total_score": scored["total_score"],
            "score_breakdown": scored["breakdown"],
            "explanation": scored["explanation"],
            "tables": [
                {
                    "alias": t["alias"],
                    "full_name": t["full_name"],
                    "name": t["name"],
                    "layer": t["layer"],
                    "description": t.get("description", "")[:150]
                }
                for t in tables
            ],
            "joins": formatted_joins,
            "columns_preview": formatted_columns,
            "total_columns": len(combo.get("columns", [])),
            "lineage_preview": lineage_preview,
            "pattern_matched": combo.get("pattern_match_count", 0) > 0,
            "warnings": []
        }

        # Add warnings for this option
        if combo.get("pattern_match_count", 0) == 0:
            recommendation["warnings"].append("Novel pattern - no existing lineage match")

        bronze_count = combo["layer_distribution"].get("bronze", 0)
        if target_layer == "gold" and bronze_count > 0:
            recommendation["warnings"].append(f"Direct Bronze → Gold ({bronze_count} bronze source(s))")

        formatted_recommendations.append(recommendation)

    return {"formatted_recommendations": formatted_recommendations}
```

### NODE 5: ddl_generator_node

```python
from datetime import datetime

def ddl_generator_node(state: AgentState) -> Dict:
    """
    Generate DDL or PySpark code based on selected option.

    Uses LLM to generate code with:
    - Proper table/column references from selected combination
    - JOIN conditions from ontology
    - Audience-appropriate comments and structure
    - Output format (SQL DDL or PySpark)
    """
    selected_idx = state["selected_option_index"]
    combo = state["scored_combinations"][selected_idx]["combination"]
    recommendation = state["formatted_recommendations"][selected_idx]

    output_format = state["output_format"]  # "sql" or "pyspark"
    audience = state["audience"]  # "business_user" or "data_engineer"
    target_layer = state["target_layer"]
    action = state["ddl_action"]

    # Generate or use provided table name
    target_table = state.get("target_table_name") or _generate_table_name(state)

    # Build context for LLM
    tables_info = "\n".join([
        f"- {t['alias']}: {t['full_name']} ({t['layer']}) - {t.get('description', 'No description')[:100]}"
        for t in recommendation["tables"]
    ])

    joins_info = "\n".join([
        f"- {j['condition']} ({j['join_type']})"
        for j in recommendation["joins"]
    ])

    columns_info = "\n".join([
        f"- {c['source']} AS {c['name']} -- {c.get('description', '')[:50]}"
        for c in combo.get("columns", [])
    ])

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

    if output_format == "sql":
        generated_ddl = _generate_sql_ddl(
            action, target_table, target_layer, combo,
            recommendation, audience, timestamp
        )
    else:  # pyspark
        generated_ddl = _generate_pyspark(
            action, target_table, target_layer, combo,
            recommendation, audience, timestamp
        )

    return {"generated_ddl": generated_ddl, "target_table_name": target_table}


def _generate_sql_ddl(action, target_table, target_layer, combo, rec, audience, timestamp) -> str:
    """Generate SQL DDL statement."""
    tables = rec["tables"]
    joins = rec["joins"]
    columns = combo.get("columns", [])

    # Build SELECT columns
    select_lines = []
    for col in columns:
        alias = col.get("alias", col["name"])
        source_col = f"{col.get('table_alias', 'T1')}.{col['name']}"
        if audience == "business_user" and col.get("description"):
            select_lines.append(f"    {source_col} AS {alias},  -- {col['description'][:40]}")
        else:
            select_lines.append(f"    {source_col} AS {alias},")

    # Remove trailing comma from last column
    if select_lines:
        select_lines[-1] = select_lines[-1].rstrip(",")

    select_clause = "\n".join(select_lines) if select_lines else "    *"

    # Build FROM clause with first table
    first_table = tables[0]
    from_clause = f"FROM {first_table['full_name']} AS {first_table['alias']}"

    # Build JOIN clauses
    join_clauses = []
    for join in joins:
        join_type = join.get("join_type", "INNER").upper()
        # Find the target table for this join
        for t in tables[1:]:
            if join["target"].startswith(t["alias"]):
                join_clauses.append(
                    f"{join_type} JOIN {t['full_name']} AS {t['alias']}\n    ON {join['condition']}"
                )
                break

    joins_sql = "\n".join(join_clauses)

    # Build header comments
    if audience == "business_user":
        header = f"""-- ============================================================
-- Create {target_layer.title()} Table: {target_table.split('.')[-1]}
-- ============================================================
-- Purpose: {rec.get('explanation', 'Combines data from multiple sources')}
-- Sources: {', '.join([t['name'] + ' (' + t['layer'] + ')' for t in tables])}
-- Generated: {timestamp}
-- ============================================================

"""
    else:
        header = f"""-- Target: {target_table}
-- Sources: {', '.join([t['alias'] + ': ' + t['full_name'] for t in tables])}
-- Generated: {timestamp}

"""

    sql = f"""{header}CREATE OR REPLACE TABLE {target_table} AS
SELECT
{select_clause}
{from_clause}
{joins_sql}
;"""

    return sql


def _generate_pyspark(action, target_table, target_layer, combo, rec, audience, timestamp) -> str:
    """Generate PySpark script."""
    tables = rec["tables"]
    joins = rec["joins"]
    columns = combo.get("columns", [])

    # Header
    if audience == "business_user":
        header = f'''"""
PySpark Script: Create {target_layer.title()} Table
Target: {target_table}
Purpose: {rec.get('explanation', 'Combines data from multiple sources')}
Sources: {', '.join([t['name'] for t in tables])}
Generated: {timestamp}
"""

'''
    else:
        header = f'''# Target: {target_table}
# Sources: {', '.join([t['alias'] + ': ' + t['full_name'] for t in tables])}
# Generated: {timestamp}

'''

    # Imports
    imports = """from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

"""

    # Load tables
    load_lines = []
    for t in tables:
        load_lines.append(f'df_{t["alias"].lower()} = spark.table("{t["full_name"]}").alias("{t["alias"]}")')

    load_section = "# Load source tables\n" + "\n".join(load_lines) + "\n\n"

    # Build joins
    if len(tables) > 1:
        join_lines = [f"# Perform joins (from ontology relationships)"]
        result_df = f"df_{tables[0]['alias'].lower()}"

        for i, join in enumerate(joins):
            join_type = join.get("join_type", "inner").lower()
            target_alias = join["target"].split(".")[0]
            join_lines.append(
                f'{result_df} = {result_df}.join(df_{target_alias.lower()}, '
                f'col("{join["source"]}") == col("{join["target"]}"), "{join_type}")'
            )
            result_df = result_df  # Keep same variable

        join_section = "\n".join(join_lines) + "\n\n"
    else:
        join_section = ""
        result_df = f"df_{tables[0]['alias'].lower()}"

    # Select columns
    select_lines = ["# Select and rename columns", "df_final = df_result.select("]
    for col_info in columns:
        alias = col_info.get("alias", col_info["name"])
        source = f'{col_info.get("table_alias", "T1")}.{col_info["name"]}'
        select_lines.append(f'    col("{source}").alias("{alias}"),')

    if columns:
        select_lines[-1] = select_lines[-1].rstrip(",")  # Remove last comma
    select_lines.append(")")

    select_section = "\n".join(select_lines) + "\n\n"

    # Write to table
    write_section = f'''# Write to {target_layer} layer
df_final.write.mode("overwrite").saveAsTable("{target_table}")

print(f"Table {{'{target_table}'}} created successfully. Row count: {{df_final.count()}}")
'''

    return header + imports + load_section + join_section + select_section + write_section
```

#### Example SQL DDL Output (Business User)
```sql
-- ============================================================
-- Create Silver Table: e2e_sap_po_material
-- ============================================================
-- Purpose: Uses high-quality source data; Simple structure
-- Sources: ekpo (bronze), mara (bronze)
-- Generated: 2025-11-28 10:30
-- ============================================================

CREATE OR REPLACE TABLE main.e2ematflow_silver.e2e_sap_po_material AS
SELECT
    T1.EBELN AS po_number,          -- Purchase Order Number
    T1.EBELP AS po_item,            -- PO Line Item
    T1.MATNR AS material_number,    -- Material Number
    T2.MAKTX AS material_desc,      -- Material Description
    T2.MTART AS material_type       -- Material Type
FROM main.e2ematflow_bronze_csv.ekpo AS T1
INNER JOIN main.e2ematflow_bronze_csv.mara AS T2
    ON T1.MATNR = T2.MATNR
;
```

#### Example PySpark Output (Data Engineer)
```python
# Target: main.e2ematflow_silver.e2e_sap_po_material
# Sources: T1: main.e2ematflow_bronze_csv.ekpo, T2: main.e2ematflow_bronze_csv.mara
# Generated: 2025-11-28 10:30

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Load source tables
df_t1 = spark.table("main.e2ematflow_bronze_csv.ekpo").alias("T1")
df_t2 = spark.table("main.e2ematflow_bronze_csv.mara").alias("T2")

# Perform joins (from ontology relationships)
df_t1 = df_t1.join(df_t2, col("T1.MATNR") == col("T2.MATNR"), "inner")

# Select and rename columns
df_final = df_result.select(
    col("T1.EBELN").alias("po_number"),
    col("T1.EBELP").alias("po_item"),
    col("T1.MATNR").alias("material_number"),
    col("T2.MAKTX").alias("material_desc"),
    col("T2.MTART").alias("material_type")
)

# Write to silver layer
df_final.write.mode("overwrite").saveAsTable("main.e2ematflow_silver.e2e_sap_po_material")

print(f"Table {'main.e2ematflow_silver.e2e_sap_po_material'} created successfully. Row count: {df_final.count()}")
```

### NODE 6: ddl_validation_node

```python
import sqlparse

def ddl_validation_node(state: AgentState) -> Dict:
    """
    Validate generated DDL/PySpark against Neo4j ontology.

    Validates:
    1. All source tables exist in Neo4j
    2. All columns exist in their respective tables
    3. All JOIN conditions exist in Neo4j relationships
    4. Layer rules are respected
    5. SQL/PySpark syntax is valid
    """
    from tools.neo4j_tool import execute_cypher

    generated_ddl = state["generated_ddl"]
    selected_idx = state["selected_option_index"]
    combo = state["scored_combinations"][selected_idx]["combination"]
    target_layer = state["target_layer"]
    output_format = state["output_format"]

    validation_result = {
        "valid": True,
        "errors": [],
        "warnings": []
    }

    # ============================================
    # 1. Validate all source tables exist
    # ============================================
    for table in combo["tables"]:
        result = execute_cypher("""
            MATCH (t:Table {full_name: $full_name})
            RETURN t.name AS name
        """, {"full_name": table["full_name"]})

        if not result:
            validation_result["valid"] = False
            validation_result["errors"].append({
                "type": "table_not_found",
                "message": f"Table not found in ontology: {table['full_name']}"
            })

    # ============================================
    # 2. Validate all columns exist in source tables
    # ============================================
    for col in combo.get("columns", []):
        source_table = col.get("source_table")
        col_name = col.get("name")

        if source_table and col_name:
            result = execute_cypher("""
                MATCH (t:Table {full_name: $table_name})-[:HAS_COLUMN]->(c:Column)
                WHERE toLower(c.name) = toLower($col_name)
                RETURN c.name AS name
            """, {"table_name": source_table, "col_name": col_name})

            if not result:
                validation_result["valid"] = False
                validation_result["errors"].append({
                    "type": "column_not_found",
                    "message": f"Column not found: {source_table}.{col_name}"
                })

    # ============================================
    # 3. Validate all JOINs exist in Neo4j relationships
    # ============================================
    for join_path in combo.get("join_paths", []):
        src_table = join_path.get("source_table")
        dst_table = join_path.get("target_table")
        src_col = join_path.get("source_column")
        dst_col = join_path.get("target_column")

        # Check if relationship exists (any direction)
        result = execute_cypher("""
            MATCH (t1:Table {full_name: $src_table})-[r:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER]-(t2:Table {full_name: $dst_table})
            WHERE (r.src_column = $src_col AND r.dst_column = $dst_col)
               OR (r.src_column = $dst_col AND r.dst_column = $src_col)
            RETURN type(r) AS rel_type
        """, {
            "src_table": src_table,
            "dst_table": dst_table,
            "src_col": src_col,
            "dst_col": dst_col
        })

        if not result:
            validation_result["valid"] = False
            validation_result["errors"].append({
                "type": "join_not_in_ontology",
                "message": f"JOIN not found in ontology: {src_table}.{src_col} = {dst_table}.{dst_col}"
            })

    # ============================================
    # 4. Validate layer rules
    # ============================================
    for table in combo["tables"]:
        source_layer = table["layer"]

        # Silver can source from: bronze, silver
        if target_layer == "silver" and source_layer == "gold":
            validation_result["valid"] = False
            validation_result["errors"].append({
                "type": "layer_rule_violation",
                "message": f"Silver tables cannot source from Gold: {table['full_name']}"
            })

        # Cannot create bronze tables
        if target_layer == "bronze":
            validation_result["valid"] = False
            validation_result["errors"].append({
                "type": "layer_rule_violation",
                "message": "Cannot create Bronze tables - they are source data only"
            })

    # ============================================
    # 5. Validate SQL syntax (for SQL output)
    # ============================================
    if output_format == "sql":
        try:
            parsed = sqlparse.parse(generated_ddl)
            if not parsed or not parsed[0].tokens:
                validation_result["warnings"].append({
                    "type": "syntax_warning",
                    "message": "SQL parsing returned empty result - verify syntax manually"
                })
        except Exception as e:
            validation_result["warnings"].append({
                "type": "syntax_warning",
                "message": f"SQL syntax check warning: {str(e)}"
            })

    # ============================================
    # 6. Add lineage pattern warnings (not errors)
    # ============================================
    lineage_warnings = _validate_lineage_patterns(combo, target_layer)
    validation_result["warnings"].extend(lineage_warnings)

    return {"ddl_validation_result": validation_result}


def _validate_lineage_patterns(combo: Dict, target_layer: str) -> List[Dict]:
    """Generate lineage warnings (not errors)."""
    warnings = []

    # Check for layer bypass (Bronze → Gold)
    if target_layer == "gold":
        bronze_sources = [t for t in combo["tables"] if t["layer"] == "bronze"]
        if bronze_sources:
            warnings.append({
                "type": "layer_bypass",
                "message": f"Direct Bronze → Gold lineage. Consider using Silver intermediate. "
                          f"Tables: {', '.join([t['name'] for t in bronze_sources])}"
            })

    # Check for novel pattern (no existing IS_SOURCE_OF)
    pattern_match_count = combo.get("pattern_match_count", 0)
    if pattern_match_count == 0:
        warnings.append({
            "type": "novel_pattern",
            "message": "Novel combination - no existing IS_SOURCE_OF lineage patterns found. Verify this is intended."
        })

    return warnings
```

### NODE 7: impact_analyzer_node (Enhanced with IS_SOURCE_OF)

```python
def impact_analyzer_node(state: AgentState) -> Dict:
    """
    Analyze impact of DDL operation using IS_SOURCE_OF lineage.

    Generates:
    1. Column details with descriptions
    2. Lineage data for visualization (using IS_SOURCE_OF)
    3. Impact summary (what will be created/changed)
    4. For DROP: trace downstream dependencies via IS_SOURCE_OF
    """
    from tools.neo4j_tool import execute_cypher

    action = state["ddl_action"]
    target_table = state.get("target_table_name") or _generate_table_name(state)
    combo = state["scored_combinations"][state["selected_option_index"]]["combination"]

    impact = {
        "action": action,
        "target_table": target_table,
        "target_layer": state["target_layer"],
        "lineage_warnings": [],
        "downstream_impact": []
    }

    # ============================================
    # Column details with descriptions
    # ============================================
    column_explanations = []
    for col in combo.get("columns", []):
        column_explanations.append({
            "name": col.get("alias", col["name"]),
            "source_column": col.get("name"),
            "source_table": col.get("source_table"),
            "data_type": col.get("data_type", "STRING"),
            "description": col.get("description", ""),
            "transformation": col.get("transformation_type", "direct")
        })

    # ============================================
    # Lineage data for visualization (IS_SOURCE_OF)
    # ============================================
    lineage_data = {"nodes": [], "edges": []}

    # Add source tables and edges
    for table in combo["tables"]:
        lineage_data["nodes"].append({
            "id": table["full_name"],
            "label": table.get("name", table["full_name"].split(".")[-1]),
            "layer": table["layer"],
            "type": "source"
        })
        lineage_data["edges"].append({
            "source": table["full_name"],
            "target": target_table,
            "relationship": "IS_SOURCE_OF"
        })

    # Add target table node
    lineage_data["nodes"].append({
        "id": target_table,
        "label": target_table.split(".")[-1],
        "layer": state["target_layer"],
        "type": "target"
    })

    # Trace upstream lineage (what feeds into source tables)
    for table in combo["tables"]:
        upstream = execute_cypher("""
            MATCH (upstream:Table)-[:IS_SOURCE_OF]->(src:Table {full_name: $table_name})
            RETURN upstream.full_name AS full_name, upstream.name AS name, upstream.layer AS layer
        """, {"table_name": table["full_name"]})

        for u in upstream:
            if not any(n["id"] == u["full_name"] for n in lineage_data["nodes"]):
                lineage_data["nodes"].append({
                    "id": u["full_name"], "label": u["name"],
                    "layer": u["layer"], "type": "upstream"
                })
                lineage_data["edges"].append({
                    "source": u["full_name"], "target": table["full_name"],
                    "relationship": "IS_SOURCE_OF"
                })

    # ============================================
    # DROP impact: find downstream dependencies
    # ============================================
    if action == "DROP":
        downstream = execute_cypher("""
            MATCH (src:Table {full_name: $table_name})-[:IS_SOURCE_OF*1..3]->(downstream:Table)
            RETURN DISTINCT downstream.full_name AS full_name, downstream.name AS name, downstream.layer AS layer
        """, {"table_name": target_table})

        impact["downstream_impact"] = [
            {"table": d["full_name"], "name": d["name"], "layer": d["layer"],
             "warning": f"⚠️ Will lose source data for {d['name']}"}
            for d in downstream
        ]

        if downstream:
            impact["lineage_warnings"].append({
                "type": "downstream_dependency",
                "message": f"⚠️ {len(downstream)} table(s) depend on this table via IS_SOURCE_OF"
            })

    # ============================================
    # Lineage pattern warnings
    # ============================================
    if combo.get("pattern_match_count", 0) == 0:
        impact["lineage_warnings"].append({
            "type": "novel_pattern",
            "message": "⚠️ Novel combination - no existing IS_SOURCE_OF patterns found"
        })

    # Layer bypass warning (Bronze → Gold)
    bronze_sources = [t for t in combo["tables"] if t["layer"] == "bronze"]
    if state["target_layer"] == "gold" and bronze_sources:
        impact["lineage_warnings"].append({
            "type": "layer_bypass",
            "message": "⚠️ Direct Bronze → Gold lineage. Consider Silver intermediate."
        })

    return {
        "impact_analysis": impact,
        "lineage_data": lineage_data,
        "column_explanations": column_explanations
    }
```

### NODE 8: execution_explainer_node

```python
def execution_explainer_node(state: AgentState) -> Dict:
    """
    Generate human-readable explanation of what will happen during execution.

    Explains:
    - What exactly will happen on Databricks
    - What tables/columns will be created or modified
    - What relationships will be affected
    - Whether this can be undone (reversibility)
    """
    action = state["ddl_action"]
    target_table = state.get("target_table_name")
    target_layer = state["target_layer"]
    output_format = state["output_format"]
    selected_idx = state["selected_option_index"]
    combo = state["scored_combinations"][selected_idx]["combination"]
    impact = state.get("impact_analysis", {})

    # Build explanation sections
    sections = []

    # Section 1: Action Summary
    if action == "CREATE":
        sections.append(f"""## What Will Happen

**Action**: CREATE new {target_layer.upper()} table
**Target**: `{target_table}`
**Format**: {output_format.upper()}

This will create a new table in your Databricks {target_layer} layer by combining data from {len(combo['tables'])} source table(s).""")

    elif action == "DROP":
        downstream_count = len(impact.get("downstream_impact", []))
        warning = f"\n⚠️ **WARNING**: {downstream_count} downstream table(s) depend on this table!" if downstream_count > 0 else ""
        sections.append(f"""## What Will Happen

**Action**: DROP existing table
**Target**: `{target_table}`{warning}

This will permanently delete the table and all its data from Databricks.""")

    elif action.startswith("ALTER"):
        sections.append(f"""## What Will Happen

**Action**: ALTER table structure
**Target**: `{target_table}`

This will modify the existing table structure.""")

    # Section 2: Source Tables
    source_list = "\n".join([
        f"  - `{t['full_name']}` ({t['layer']})"
        for t in combo["tables"]
    ])
    sections.append(f"""## Source Tables

{source_list}""")

    # Section 3: Columns Created
    col_count = len(combo.get("columns", []))
    sections.append(f"""## Columns

**{col_count}** columns will be created in the new table.""")

    # Section 4: Lineage Impact
    if action == "CREATE":
        sections.append(f"""## Lineage Impact

After execution, the following will be added to Neo4j ontology:
- 1 new Table node: `{target_table}`
- {col_count} new Column nodes with HAS_COLUMN relationships
- {len(combo['tables'])} new IS_SOURCE_OF (table) relationships
- Up to {col_count} new IS_SOURCE_OF (column) relationships""")

    # Section 5: Warnings
    warnings = impact.get("lineage_warnings", [])
    if warnings:
        warning_list = "\n".join([f"  - {w['message']}" for w in warnings])
        sections.append(f"""## Warnings

{warning_list}""")

    # Section 6: Reversibility
    if action == "CREATE":
        sections.append("""## Reversibility

**Can be undone**: Yes
- Table can be dropped with `DROP TABLE` command
- Ontology entries can be removed manually or via cleanup script""")
    elif action == "DROP":
        sections.append("""## Reversibility

**Can be undone**: NO - This action is PERMANENT
- Data will be lost and cannot be recovered
- You would need to recreate the table from source data""")

    execution_explanation = "\n\n".join(sections)

    return {"execution_explanation": execution_explanation}
```

### NODE 9: ddl_executor_node

```python
def ddl_executor_node(state: AgentState) -> Dict:
    """
    Execute the DDL/PySpark on Databricks.

    Handles:
    - Connection to Databricks
    - Execution of SQL DDL or PySpark script
    - Capturing results and errors
    - Row count verification
    """
    from tools.databricks_connection import get_databricks_connection

    generated_ddl = state["generated_ddl"]
    output_format = state["output_format"]
    target_table = state.get("target_table_name")

    execution_result = {
        "success": False,
        "message": "",
        "row_count": None,
        "execution_time_ms": None,
        "error": None
    }

    try:
        import time
        start_time = time.time()

        # Get Databricks connection
        connection = get_databricks_connection()

        if output_format == "sql":
            # Execute SQL DDL
            cursor = connection.cursor()
            cursor.execute(generated_ddl)

            # Get row count for CREATE TABLE AS SELECT
            if state["ddl_action"] == "CREATE":
                cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
                row_count = cursor.fetchone()[0]
                execution_result["row_count"] = row_count

            cursor.close()

        else:  # pyspark
            # For PySpark, we need to execute via Databricks Jobs API or notebook
            # This is a simplified version - real implementation would use Jobs API
            result = connection.execute_pyspark(generated_ddl)

            if result.get("success"):
                execution_result["row_count"] = result.get("row_count")
            else:
                raise Exception(result.get("error", "PySpark execution failed"))

        end_time = time.time()
        execution_result["execution_time_ms"] = int((end_time - start_time) * 1000)
        execution_result["success"] = True
        execution_result["message"] = f"Successfully created table {target_table}"

        if execution_result["row_count"]:
            execution_result["message"] += f" with {execution_result['row_count']:,} rows"

    except Exception as e:
        execution_result["success"] = False
        execution_result["error"] = str(e)
        execution_result["message"] = f"Execution failed: {str(e)}"

    return {"execution_result": execution_result}
```

### NODE 10: ontology_sync_node

```python
def ontology_sync_node(state: AgentState) -> Dict:
    """Update Neo4j ontology after successful DDL execution."""

    from tools.neo4j_tool import execute_cypher

    action = state["ddl_action"]
    target_table = state.get("target_table_name") or _generate_table_name(state)
    combo = state["scored_combinations"][state["selected_option_index"]]["combination"]

    # Extract catalog, schema, layer from full table name
    # Format: catalog.schema.tablename (e.g., main.e2ematflow_silver.my_table)
    parts = target_table.split(".")
    if len(parts) == 3:
        catalog, schema, table_name = parts
    else:
        catalog = "main"
        schema = parts[0] if len(parts) >= 2 else "unknown"
        table_name = parts[-1]

    try:
        if action == "CREATE":
            # 1. Create Table node with all properties
            execute_cypher("""
                CREATE (t:Table {
                    id: $table_id,
                    name: $name,
                    full_name: $full_name,
                    catalog: $catalog,
                    schema: $schema,
                    layer: $layer,
                    description: $description
                })
            """, {
                "table_id": target_table.replace(".", "_"),
                "name": table_name,
                "full_name": target_table,
                "catalog": catalog,
                "schema": schema,
                "layer": state["target_layer"],
                "description": f"Generated {state['target_layer']} table"
            })

            # 2. Create Column nodes
            for col in combo.get("columns", []):
                execute_cypher("""
                    MATCH (t:Table {full_name: $table_name})
                    CREATE (t)-[:HAS_COLUMN]->(c:Column {
                        name: $col_name,
                        data_type: $data_type,
                        description: $description
                    })
                """, {
                    "table_name": target_table,
                    "col_name": col.get("alias", col["name"]),
                    "data_type": col.get("data_type", "STRING"),
                    "description": col.get("description", "")
                })

            # 3. Create lineage relationships (IS_SOURCE_OF)
            # Direction: source IS_SOURCE_OF target (source feeds into target)
            for table in combo["tables"]:
                execute_cypher("""
                    MATCH (src:Table {full_name: $source})
                    MATCH (tgt:Table {full_name: $target})
                    MERGE (src)-[r:IS_SOURCE_OF]->(tgt)
                    SET r.source_layer = src.layer,
                        r.destination_layer = tgt.layer,
                        r.lineage_type = 'table'
                """, {"source": table["full_name"], "target": target_table})

            # 4. Create column lineage (IS_SOURCE_OF between columns)
            for col in combo.get("columns", []):
                if col.get("source_table") and col.get("source_column"):
                    execute_cypher("""
                        MATCH (src_t:Table {full_name: $src_table})-[:HAS_COLUMN]->(src_c:Column)
                        WHERE toLower(src_c.name) = toLower($src_col)
                        MATCH (dst_t:Table {full_name: $dst_table})-[:HAS_COLUMN]->(dst_c:Column)
                        WHERE toLower(dst_c.name) = toLower($dst_col)
                        MERGE (src_c)-[r:IS_SOURCE_OF]->(dst_c)
                        SET r.src_table = $src_table,
                            r.dst_table = $dst_table,
                            r.src_layer = src_t.layer,
                            r.dst_layer = dst_t.layer,
                            r.transformation_type = $transform_type,
                            r.lineage_type = 'column'
                    """, {
                        "src_table": col["source_table"],
                        "src_col": col["source_column"],
                        "dst_table": target_table,
                        "dst_col": col.get("alias", col["name"]),
                        "transform_type": col.get("transformation_type", "direct")
                    })

            # 5. Generate embeddings for semantic search
            _generate_embeddings_for_table(target_table)

        elif action == "DROP":
            execute_cypher("""
                MATCH (t:Table {full_name: $table_name})
                OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
                DETACH DELETE t, c
            """, {"table_name": target_table})

        return {
            "ontology_sync_result": {
                "success": True,
                "message": f"Neo4j ontology updated for {action} on {target_table}"
            }
        }
    except Exception as e:
        return {
            "ontology_sync_result": {
                "success": False,
                "error": str(e)
            }
        }
```

### NODE 11: ontology_validation_node (Enhanced with IS_SOURCE_OF)

```python
def ontology_validation_node(state: AgentState) -> Dict:
    """
    Validate Neo4j ontology after sync, including IS_SOURCE_OF lineage.

    Validates:
    1. Graph integrity (no orphan nodes)
    2. Embeddings generated for new table
    3. IS_SOURCE_OF relationships created correctly
    4. No broken lineage chains
    """
    from tools.neo4j_tool import execute_cypher

    target_table = state.get("target_table_name") or _generate_table_name(state)
    validation_result = {
        "valid": True,
        "checks": [],
        "warnings": []
    }

    # ============================================
    # Check 1: Table exists with required properties
    # ============================================
    table_check = execute_cypher("""
        MATCH (t:Table {full_name: $table_name})
        RETURN t.name AS name, t.layer AS layer, t.catalog AS catalog,
               t.schema AS schema, t.description IS NOT NULL AS has_desc
    """, {"table_name": target_table})

    if table_check:
        validation_result["checks"].append({
            "name": "table_exists",
            "status": "passed",
            "message": f"Table {target_table} exists with required properties"
        })
    else:
        validation_result["valid"] = False
        validation_result["checks"].append({
            "name": "table_exists",
            "status": "failed",
            "message": f"Table {target_table} not found in Neo4j"
        })

    # ============================================
    # Check 2: Columns exist (HAS_COLUMN relationships)
    # ============================================
    column_count = execute_cypher("""
        MATCH (t:Table {full_name: $table_name})-[:HAS_COLUMN]->(c:Column)
        RETURN count(c) AS count
    """, {"table_name": target_table})

    col_count = column_count[0]["count"] if column_count else 0
    if col_count > 0:
        validation_result["checks"].append({
            "name": "columns_exist",
            "status": "passed",
            "message": f"{col_count} columns created with HAS_COLUMN relationships"
        })
    else:
        validation_result["warnings"].append({
            "name": "no_columns",
            "message": "No columns found for new table"
        })

    # ============================================
    # Check 3: IS_SOURCE_OF table lineage created
    # ============================================
    table_lineage = execute_cypher("""
        MATCH (src:Table)-[r:IS_SOURCE_OF]->(t:Table {full_name: $table_name})
        WHERE r.lineage_type = 'table'
        RETURN count(r) AS count
    """, {"table_name": target_table})

    lineage_count = table_lineage[0]["count"] if table_lineage else 0
    if lineage_count > 0:
        validation_result["checks"].append({
            "name": "table_lineage",
            "status": "passed",
            "message": f"{lineage_count} IS_SOURCE_OF (table) relationships created"
        })
    else:
        validation_result["warnings"].append({
            "name": "no_table_lineage",
            "message": "No IS_SOURCE_OF table lineage found - may be intentional for root tables"
        })

    # ============================================
    # Check 4: IS_SOURCE_OF column lineage created
    # ============================================
    column_lineage = execute_cypher("""
        MATCH (t:Table {full_name: $table_name})-[:HAS_COLUMN]->(dst:Column)
        MATCH (src:Column)-[r:IS_SOURCE_OF]->(dst)
        WHERE r.lineage_type = 'column'
        RETURN count(r) AS count
    """, {"table_name": target_table})

    col_lineage_count = column_lineage[0]["count"] if column_lineage else 0
    if col_lineage_count > 0:
        validation_result["checks"].append({
            "name": "column_lineage",
            "status": "passed",
            "message": f"{col_lineage_count} IS_SOURCE_OF (column) relationships created"
        })
    else:
        validation_result["warnings"].append({
            "name": "no_column_lineage",
            "message": "No IS_SOURCE_OF column lineage found"
        })

    # ============================================
    # Check 5: No orphan columns (columns without parent table)
    # ============================================
    orphan_check = execute_cypher("""
        MATCH (c:Column)
        WHERE NOT (c)<-[:HAS_COLUMN]-(:Table)
        RETURN count(c) AS orphan_count
    """)

    orphan_count = orphan_check[0]["orphan_count"] if orphan_check else 0
    if orphan_count == 0:
        validation_result["checks"].append({
            "name": "no_orphans",
            "status": "passed",
            "message": "No orphan columns detected"
        })
    else:
        validation_result["warnings"].append({
            "name": "orphan_columns",
            "message": f"{orphan_count} orphan column(s) detected in graph"
        })

    # ============================================
    # Check 6: Embeddings generated
    # ============================================
    embedding_check = execute_cypher("""
        MATCH (t:Table {full_name: $table_name})
        RETURN t.description_embedding IS NOT NULL AS has_embedding
    """, {"table_name": target_table})

    has_embedding = embedding_check[0]["has_embedding"] if embedding_check else False
    if has_embedding:
        validation_result["checks"].append({
            "name": "embeddings",
            "status": "passed",
            "message": "Embeddings generated for semantic search"
        })
    else:
        validation_result["warnings"].append({
            "name": "no_embeddings",
            "message": "Embeddings not generated - semantic search may not find this table"
        })

    return {"ontology_validation_result": validation_result}
```

### NODE 12: format_ddl_result_node

```python
def format_ddl_result_node(state: AgentState) -> Dict:
    """
    Format the final response to the user.

    Handles four scenarios:
    1. Discovery error - show error and suggestion
    2. Execution successful - show success message with details
    3. Execution failed - show error details
    4. Execution cancelled - show generated code for manual use
    """
    # ============================================
    # Scenario 0: Discovery error (from multi_signal_discovery)
    # ============================================
    discovery_error = state.get("discovery_error")
    if discovery_error:
        error_code = discovery_error.get("error", "UNKNOWN_ERROR")
        message = discovery_error.get("message", "An error occurred during table discovery.")
        suggestion = discovery_error.get("suggestion", "Please try rephrasing your request.")

        sections = [f"""## Table Discovery Failed

**Error Code**: `{error_code}`

**Message**: {message}

**Suggestion**: {suggestion}

### What You Can Do

1. Try specifying more concrete table names (e.g., "purchase orders", "materials")
2. Use column names that exist in the ontology
3. Check that the target layer is 'silver' or 'gold'
"""]
        # Include discovery warnings if any
        warnings = state.get("discovery_warnings", [])
        if warnings:
            warning_msgs = "\n".join([f"  - {w.get('message', str(w))}" for w in warnings])
            sections.append(f"""### Additional Warnings

{warning_msgs}""")

        return {"final_response": "\n\n".join(sections)}

    execution_approved = state.get("execution_approved", False)
    execution_result = state.get("execution_result", {})
    ontology_sync_result = state.get("ontology_sync_result", {})
    ontology_validation_result = state.get("ontology_validation_result", {})
    generated_ddl = state.get("generated_ddl", "")
    target_table = state.get("target_table_name", "unknown")
    output_format = state.get("output_format", "sql")

    sections = []

    # ============================================
    # Scenario 1: Execution was cancelled
    # ============================================
    if not execution_approved:
        sections.append(f"""## Execution Cancelled

The DDL was **not executed** on Databricks as per your request.

### Generated {output_format.upper()} Code

You can copy this code and execute it manually:

```{output_format}
{generated_ddl}
```

**Note**: The Neo4j ontology was NOT updated since execution was cancelled.""")

        return {"final_response": "\n\n".join(sections)}

    # ============================================
    # Scenario 2: Execution failed
    # ============================================
    if not execution_result.get("success", False):
        error_msg = execution_result.get("error", "Unknown error")
        sections.append(f"""## Execution Failed

**Error**: {error_msg}

### Generated {output_format.upper()} Code

```{output_format}
{generated_ddl}
```

**Note**: The Neo4j ontology was NOT updated due to execution failure.

### Troubleshooting
- Check if the source tables exist in Databricks
- Verify you have permission to create tables in the target schema
- Check the Databricks cluster is running""")

        return {"final_response": "\n\n".join(sections)}

    # ============================================
    # Scenario 3: Execution successful
    # ============================================
    row_count = execution_result.get("row_count")
    exec_time = execution_result.get("execution_time_ms")

    sections.append(f"""## Execution Successful

**Table Created**: `{target_table}`
**Row Count**: {row_count:,} rows
**Execution Time**: {exec_time}ms""")

    # Ontology sync status
    if ontology_sync_result.get("success"):
        sections.append("""### Neo4j Ontology Updated

The following was added to the ontology:
- New Table node with metadata
- Column nodes with HAS_COLUMN relationships
- IS_SOURCE_OF lineage relationships (table and column level)
- Embeddings for semantic search""")
    else:
        sync_error = ontology_sync_result.get("error", "Unknown error")
        sections.append(f"""### Neo4j Ontology Update Failed

**Warning**: The table was created in Databricks but the ontology sync failed.
**Error**: {sync_error}

Please run the ontology sync manually or contact support.""")

    # Ontology validation results
    validation = ontology_validation_result
    if validation.get("valid"):
        passed_checks = [c for c in validation.get("checks", []) if c["status"] == "passed"]
        sections.append(f"""### Ontology Validation Passed

All {len(passed_checks)} validation checks passed.""")
    else:
        failed_checks = [c for c in validation.get("checks", []) if c["status"] == "failed"]
        if failed_checks:
            check_msgs = "\n".join([f"  - {c['message']}" for c in failed_checks])
            sections.append(f"""### Ontology Validation Warnings

Some validation checks failed:
{check_msgs}""")

    # Show warnings if any
    warnings = validation.get("warnings", [])
    if warnings:
        warning_msgs = "\n".join([f"  - {w['message']}" for w in warnings])
        sections.append(f"""### Warnings

{warning_msgs}""")

    # Show the executed code
    sections.append(f"""### Executed {output_format.upper()} Code

```{output_format}
{generated_ddl}
```""")

    # Next steps
    sections.append(f"""### Next Steps

1. Query your new table: `SELECT * FROM {target_table} LIMIT 10`
2. The table is now searchable via the NL-to-SQL agent
3. Lineage is tracked - you can trace data back to source tables""")

    return {"final_response": "\n\n".join(sections)}
```

### Helper Functions

```python
from typing import List, Dict
from itertools import product


def _generate_table_combinations(concept_tables: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Generate all viable combinations of tables across concepts.

    For example, if concept_tables = {
        "purchase_orders": [T1, T2],
        "materials": [T3, T4]
    }
    Returns combinations like: [T1, T3], [T1, T4], [T2, T3], [T2, T4]
    """
    if not concept_tables:
        return []

    # Get all concepts and their table lists
    concepts = list(concept_tables.keys())
    table_lists = [concept_tables[c] for c in concepts]

    # Generate cartesian product of all table combinations
    combinations = []
    for combo in product(*table_lists):
        # Filter out duplicates (same table appearing for multiple concepts)
        unique_tables = []
        seen_names = set()
        for table in combo:
            if table["full_name"] not in seen_names:
                unique_tables.append(table)
                seen_names.add(table["full_name"])

        # Only include if we have tables from all concepts
        if len(unique_tables) >= len(concepts):
            combinations.append({
                "tables": unique_tables,
                "concepts_covered": concepts
            })

    return combinations


def _find_join_paths(tables: List[Dict]) -> List[Dict]:
    """
    Find valid JOIN paths between tables using Neo4j ontology.

    Returns list of join conditions that exist in the ontology.
    """
    from tools.neo4j_tool import execute_cypher

    if len(tables) < 2:
        return []  # No joins needed for single table

    join_paths = []
    table_names = [t["full_name"] for t in tables]

    # Query Neo4j for relationships between these tables
    result = execute_cypher("""
        MATCH (t1:Table)-[r:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER]-(t2:Table)
        WHERE t1.full_name IN $tables AND t2.full_name IN $tables
        AND t1.full_name < t2.full_name  -- Avoid duplicates
        RETURN t1.full_name AS source_table,
               t2.full_name AS target_table,
               r.src_column AS source_column,
               r.dst_column AS target_column,
               type(r) AS join_type,
               r.join_condition AS join_condition
    """, {"tables": table_names})

    for row in result:
        join_paths.append({
            "source_table": row["source_table"],
            "target_table": row["target_table"],
            "source_column": row["source_column"],
            "target_column": row["target_column"],
            "join_type": row["join_type"],
            "join_condition": row["join_condition"]
        })

    return join_paths


def _get_relevant_columns(tables: List[Dict], concepts: List[str]) -> List[Dict]:
    """
    Get relevant columns from tables for the DDL.

    Uses semantic search to find columns matching the concepts.
    """
    from tools.neo4j_tool import execute_cypher
    from tools.graphrag_retriever import get_graphrag_retriever

    columns = []
    retriever = get_graphrag_retriever()

    for table in tables:
        # Get all columns for the table
        result = execute_cypher("""
            MATCH (t:Table {full_name: $table_name})-[:HAS_COLUMN]->(c:Column)
            RETURN c.name AS name,
                   c.data_type AS data_type,
                   c.description AS description
        """, {"table_name": table["full_name"]})

        for col in result:
            include = False  # Will be set True if column matches a concept

            # Check if column matches any concept
            if not include:
                col_text = f"{col['name']} {col.get('description', '')}".lower()
                for concept in concepts:
                    if concept.lower() in col_text:
                        include = True
                        break

            if include:
                columns.append({
                    "name": col["name"],
                    "source_table": table["full_name"],
                    "source_column": col["name"],
                    "table_alias": table.get("alias", "T1"),
                    "data_type": col.get("data_type", "STRING"),
                    "description": col.get("description", "")
                })

    return columns


def _generate_table_name(state: Dict) -> str:
    """
    Generate a table name based on the DDL request.

    Format: catalog.schema.tablename
    Example: main.e2ematflow_silver.e2e_sap_po_material
    """
    target_layer = state["target_layer"]
    concepts = state.get("requested_concepts", [])

    # Build schema based on layer
    schema_map = {
        "bronze": "e2ematflow_bronze_csv",
        "silver": "e2ematflow_silver",
        "gold": "e2ematflow_gold"
    }
    schema = schema_map.get(target_layer, "e2ematflow_silver")

    # Build table name from concepts
    # Clean concepts: remove spaces, special chars, join with underscore
    clean_concepts = []
    for concept in concepts[:3]:  # Max 3 concepts in name
        clean = concept.lower().replace(" ", "_").replace("-", "_")
        clean = ''.join(c for c in clean if c.isalnum() or c == '_')
        clean_concepts.append(clean)

    table_name = "_".join(clean_concepts) if clean_concepts else "new_table"
    table_name = f"e2e_sap_{table_name}"

    return f"main.{schema}.{table_name}"


def _generate_embeddings_for_table(table_name: str) -> bool:
    """
    Generate embeddings for a new table to enable semantic search.

    Uses OpenAI embeddings via the graphrag library.
    """
    from tools.neo4j_tool import execute_cypher
    import openai
    import os

    try:
        # Get table description
        result = execute_cypher("""
            MATCH (t:Table {full_name: $table_name})
            RETURN t.description AS description, t.name AS name
        """, {"table_name": table_name})

        if not result:
            return False

        description = result[0].get("description", result[0]["name"])

        # Generate embedding using OpenAI
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=description
        )
        embedding = response.data[0].embedding

        # Store embedding in Neo4j
        execute_cypher("""
            MATCH (t:Table {full_name: $table_name})
            SET t.description_embedding = $embedding
        """, {"table_name": table_name, "embedding": embedding})

        return True

    except Exception as e:
        print(f"Warning: Failed to generate embeddings for {table_name}: {e}")
        return False


def get_graphrag_retriever():
    """
    Get the GraphRAG retriever for semantic search.

    Returns a retriever configured for the Neo4j ontology.
    """
    from tools.graphrag_retriever import GraphRAGRetriever
    import os

    return GraphRAGRetriever(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password123")
    )
```

---

## 10. HITL Interrupt Nodes

### Concept Clarification Interrupt (NEW in v3.0)

This gate handles cases where the multi-signal discovery needs user clarification about ambiguous concepts.

```python
def concept_clarification_interrupt_node(state: AgentState) -> Dict:
    """HITL interrupt for concept clarification when discovery is ambiguous."""

    clarification = state.get("needs_clarification", {})

    if not clarification:
        # No clarification needed, shouldn't reach here
        return {}

    user_decision = interrupt({
        "type": "concept_clarification_required",
        "message": clarification.get("question", "Please clarify your request"),
        "clarification_type": clarification.get("clarification_type"),
        "options": clarification.get("options", []),
        "columns_found": clarification.get("columns_found", [])
    })

    # User selects an option or provides custom input
    selected = user_decision.get("selected_option") or user_decision.get("custom_input")

    return {
        "user_clarification_response": selected,
        "needs_clarification": None  # Clear the clarification request
    }
```

### Option Selection Interrupt

```python
def option_selection_interrupt_node(state: AgentState) -> Dict:
    """HITL interrupt for option selection (max 5 iterations)."""

    iteration = state.get("feedback_iteration", 0)

    if iteration >= 5:
        # Max iterations reached, force selection of top option
        return {"selected_option_index": 0, "user_feedback": None}

    user_decision = interrupt({
        "type": "option_selection_required",
        "message": "Please select an option or provide feedback",
        "options": state.get("formatted_recommendations", []),
        "iteration": iteration,
        "max_iterations": 5
    })

    # Expected: {"action": "select|select_with_feedback|reject", ...}
    if user_decision["action"] == "select":
        return {"selected_option_index": user_decision["option_index"], "user_feedback": None}
    elif user_decision["action"] == "select_with_feedback":
        return {"selected_option_index": user_decision["option_index"], "user_feedback": user_decision.get("feedback")}
    else:  # reject
        return {"selected_option_index": None, "user_feedback": user_decision.get("feedback"), "feedback_iteration": iteration + 1}
```

### Code Approval Interrupt

```python
def code_approval_interrupt_node(state: AgentState) -> Dict:
    """HITL interrupt for code approval."""

    user_decision = interrupt({
        "type": "code_approval_required",
        "message": "Review the generated code and impact analysis",
        "ddl": state["generated_ddl"],
        "impact": state["impact_analysis"],
        "lineage": state["lineage_data"],
        "columns": state["column_explanations"]
    })

    if user_decision["action"] == "approve":
        return {"code_approved": True}
    else:
        return {
            "code_approved": False,
            "user_feedback": user_decision.get("feedback"),
            "feedback_iteration": state.get("feedback_iteration", 0) + 1
        }
```

### Execution Approval Interrupt

```python
def execution_approval_interrupt_node(state: AgentState) -> Dict:
    """HITL interrupt for execution approval."""

    user_decision = interrupt({
        "type": "execution_approval_required",
        "message": "Confirm execution on Databricks",
        "explanation": state["execution_explanation"],
        "ddl": state["generated_ddl"],
        "warning": "This will modify your Databricks environment"
    })

    return {"execution_approved": user_decision["action"] == "execute"}
```

---

## 11. Workflow Graph Updates

**File**: `agent/workflow.py`

### Routing Functions

```python
def route_discovery_result(state: AgentState) -> str:
    """Route based on multi_signal_discovery output."""
    if state.get("discovery_error"):
        return "error"
    if state.get("needs_clarification"):
        return "needs_clarification"
    return "success"


def route_option_selection(state: AgentState) -> str:
    """Route based on user's option selection."""
    if state.get("selected_option_index") is None and state.get("user_feedback"):
        return "regenerate"
    return "continue"


def route_validation(state: AgentState) -> str:
    """Route based on DDL validation result."""
    validation = state.get("ddl_validation_result", {})
    if validation.get("errors"):
        return "invalid"
    return "valid"


def route_code_approval(state: AgentState) -> str:
    """Route based on code approval decision."""
    if state.get("code_approved"):
        return "approved"
    return "rejected"


def route_execution_approval(state: AgentState) -> str:
    """Route based on execution approval decision."""
    if state.get("execution_approved"):
        return "execute"
    return "cancel"


def route_execution_result(state: AgentState) -> str:
    """Route based on execution result."""
    result = state.get("execution_result", {})
    if result.get("success"):
        return "success"
    return "failed"
```

### Workflow Definition

```python
def build_workflow():
    workflow = StateGraph(AgentState)

    # ============================================
    # EXISTING NODES (unchanged)
    # ============================================
    workflow.add_node("query_classifier", query_classifier_node)
    workflow.add_node("metadata_query", metadata_query_node)
    workflow.add_node("parse_intent", parse_intent_node)
    workflow.add_node("fetch_joins", fetch_joins_node)
    workflow.add_node("generate_sql", generate_sql_node)
    workflow.add_node("validate_sql", validate_sql_node)
    workflow.add_node("human_approval", human_approval_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("format_results", format_results_node)
    # ... other existing nodes ...

    # ============================================
    # NEW DDL NODES
    # ============================================
    workflow.add_node("ddl_intent_parser", ddl_intent_parser_node)
    workflow.add_node("multi_signal_discovery", multi_signal_discovery_node)  # v3.0 renamed
    workflow.add_node("concept_clarification_gate", concept_clarification_interrupt_node)  # NEW: HITL for clarification
    workflow.add_node("combination_scorer", combination_scorer_node)
    workflow.add_node("recommendation_presenter", recommendation_presenter_node)
    workflow.add_node("option_selection_gate", option_selection_interrupt_node)
    workflow.add_node("ddl_generator", ddl_generator_node)
    workflow.add_node("ddl_validation", ddl_validation_node)
    workflow.add_node("impact_analyzer", impact_analyzer_node)
    workflow.add_node("code_approval_gate", code_approval_interrupt_node)
    workflow.add_node("execution_explainer", execution_explainer_node)
    workflow.add_node("execution_approval_gate", execution_approval_interrupt_node)
    workflow.add_node("ddl_executor", ddl_executor_node)
    workflow.add_node("ontology_sync", ontology_sync_node)
    workflow.add_node("ontology_validation", ontology_validation_node)
    workflow.add_node("format_ddl_result", format_ddl_result_node)

    # ============================================
    # ROUTING FROM CLASSIFIER
    # ============================================
    workflow.add_conditional_edges(
        "query_classifier",
        route_by_intent,
        {
            # EXISTING routes (unchanged)
            "metadata": "metadata_query",
            "simple_sql": "parse_intent",
            "complex_sql": "parse_intent",
            "follow_up": "parse_intent",
            "visualization": "visualization_node",
            "genealogy": "genealogy_node",

            # NEW routes
            "ddl_generation": "ddl_intent_parser",
            "pyspark_generation": "ddl_intent_parser",
        }
    )

    # ============================================
    # DDL FLOW EDGES
    # ============================================
    workflow.add_edge("ddl_intent_parser", "multi_signal_discovery")

    # NEW: Conditional routing from multi_signal_discovery (handles errors/clarification)
    workflow.add_conditional_edges(
        "multi_signal_discovery",
        route_discovery_result,
        {
            "success": "combination_scorer",
            "error": "format_ddl_result",
            "needs_clarification": "concept_clarification_gate"
        }
    )

    # Route from clarification gate back to discovery (with user's answer)
    workflow.add_edge("concept_clarification_gate", "multi_signal_discovery")

    workflow.add_edge("combination_scorer", "recommendation_presenter")
    workflow.add_edge("recommendation_presenter", "option_selection_gate")

    workflow.add_conditional_edges(
        "option_selection_gate",
        route_option_selection,
        {"regenerate": "multi_signal_discovery", "continue": "ddl_generator"}
    )

    workflow.add_edge("ddl_generator", "ddl_validation")

    workflow.add_conditional_edges(
        "ddl_validation",
        route_validation,
        {"valid": "impact_analyzer", "invalid": "recommendation_presenter"}
    )

    workflow.add_edge("impact_analyzer", "code_approval_gate")

    workflow.add_conditional_edges(
        "code_approval_gate",
        route_code_approval,
        {"approved": "execution_explainer", "rejected": "multi_signal_discovery"}
    )

    workflow.add_edge("execution_explainer", "execution_approval_gate")

    workflow.add_conditional_edges(
        "execution_approval_gate",
        route_execution_approval,
        {"execute": "ddl_executor", "cancel": "format_ddl_result"}
    )

    workflow.add_conditional_edges(
        "ddl_executor",
        route_execution_result,
        {"success": "ontology_sync", "failed": "format_ddl_result"}
    )

    workflow.add_edge("ontology_sync", "ontology_validation")
    workflow.add_edge("ontology_validation", "format_ddl_result")
    workflow.add_edge("format_ddl_result", END)

    return workflow.compile()
```

---

## 12. Validation Rules

### Table Validation
| Rule | Description |
|------|-------------|
| Existence | All source tables must exist in Neo4j ontology |
| Layer | Target layer must follow rules (silver from bronze/silver, gold from any) |
| No Cross-Layer Violation | Cannot create bronze tables |

### JOIN Validation
| Rule | Description |
|------|-------------|
| Ontology Only | Every JOIN must exist in Neo4j relationships |
| No Fabrication | System cannot invent JOINs not in ontology |
| Type Support | RELATED_TO, JOINED_ON, LEFT_OUTER, INNER |

### Column Validation
| Rule | Description |
|------|-------------|
| Existence | All columns must exist in source table Column nodes |
| Type Match | Data types must match Column node definitions |

### Lineage Validation (NEW - Warnings Only)

These are **warnings** (not errors) to inform users when their DDL deviates from established patterns.

| Warning | Condition | Message |
|---------|-----------|---------|
| Layer Bypass | Bronze → Gold without Silver | "⚠️ This lineage bypasses Silver layer. Existing patterns show Bronze → Silver → Gold." |
| Novel Pattern | No matching IS_SOURCE_OF exists | "⚠️ Novel combination - no existing lineage patterns found. Verify this is intended." |
| Column Type Mismatch | Column types differ across lineage | "⚠️ Column {name} has different types: {type1} vs {type2}" |

**Implementation:**
```python
def _validate_lineage_patterns(combo: Dict, target_layer: str) -> List[Dict]:
    """Generate lineage warnings (not errors)."""
    warnings = []

    # Check for layer bypass (Bronze → Gold)
    if target_layer == "gold":
        bronze_sources = [t for t in combo["tables"] if t["layer"] == "bronze"]
        if bronze_sources:
            warnings.append({
                "type": "layer_bypass",
                "message": "This lineage bypasses Silver layer. Existing patterns show Bronze → Silver → Gold.",
                "tables": [t["full_name"] for t in bronze_sources]
            })

    # Check for novel pattern (no existing IS_SOURCE_OF)
    pattern_match_count = combo.get("pattern_match_count", 0)
    if pattern_match_count == 0:
        warnings.append({
            "type": "novel_pattern",
            "message": "Novel combination - no existing lineage patterns found. Verify this is intended."
        })

    return warnings
```

---

## 13. Neo4j Query Reference

### Find Source Tables for Gold
```cypher
MATCH (t:Table)
WHERE t.full_name CONTAINS 'silver'
AND t.description CONTAINS $search_term
RETURN t.full_name, t.description
ORDER BY t.description
```

### Find All JOINs Between Tables
```cypher
MATCH (t1:Table)-[r:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER]-(t2:Table)
WHERE t1.full_name IN $tables AND t2.full_name IN $tables
RETURN
    t1.full_name AS source,
    t2.full_name AS target,
    r.src_column AS src_col,
    r.dst_column AS dst_col,
    r.join_condition AS join_condition,
    type(r) AS join_type
```

### Get Columns for Table
```cypher
MATCH (t:Table {full_name: $table_name})-[:HAS_COLUMN]->(c:Column)
RETURN c.name, c.data_type, c.description
ORDER BY c.name
```

### Find Downstream Dependencies (for DROP impact) - UPDATED
```cypher
-- Changed from FEEDS_INTO to IS_SOURCE_OF
MATCH (t:Table {full_name: $table_name})-[:IS_SOURCE_OF]->(downstream:Table)
RETURN downstream.full_name AS dependent_table, downstream.layer AS layer
```

### Validate No Orphan Columns
```cypher
MATCH (c:Column)
WHERE NOT (c)<-[:HAS_COLUMN]-(:Table)
RETURN c.name AS orphan_column
```

### IS_SOURCE_OF Lineage Queries (NEW)

#### Find Tables That Feed a Target Layer
```cypher
-- Find all tables that feed gold layer (for source discovery)
MATCH (src:Table)-[:IS_SOURCE_OF]->(dst:Table {layer: $target_layer})
WHERE src.layer IN $allowed_layers
RETURN DISTINCT src.full_name, src.layer, src.description,
       count(dst) AS feeds_count
ORDER BY feeds_count DESC
```

#### Check Pattern Match for Combination
```cypher
-- Check if source tables already feed the target layer
MATCH (src:Table)-[:IS_SOURCE_OF]->(dst:Table {layer: $target_layer})
WHERE src.full_name IN $source_tables
RETURN count(DISTINCT src) AS matched_sources
```

#### Full Lineage Path (Bronze → Silver → Gold)
```cypher
-- Trace complete lineage path
MATCH path = (bronze:Table {layer: 'bronze'})-[:IS_SOURCE_OF*1..2]->(gold:Table {layer: 'gold'})
WHERE gold.full_name = $target_table
RETURN [n IN nodes(path) | n.name + ' (' + n.layer + ')'] AS lineage_path
```

#### Column Lineage Trace
```cypher
-- Trace a column's lineage back to bronze
MATCH (dst_col:Column)<-[:HAS_COLUMN]-(dst:Table {full_name: $target_table})
WHERE dst_col.name = $column_name
MATCH path = (src_col:Column)-[:IS_SOURCE_OF*1..2]->(dst_col)
MATCH (src_t:Table)-[:HAS_COLUMN]->(src_col)
RETURN src_t.name + '.' + src_col.name AS source,
       dst.name + '.' + dst_col.name AS destination,
       length(path) AS hops
```

#### Lineage Statistics by Layer Transition
```cypher
-- Count lineage relationships by layer transition
MATCH (src:Table)-[r:IS_SOURCE_OF]->(dst:Table)
RETURN r.source_layer + ' → ' + r.destination_layer AS transition,
       count(*) AS count
ORDER BY count DESC
```

---

## 14. Streamlit UI Components

### Option Selection UI

```python
def render_option_selection(options: List[Dict], iteration: int):
    st.subheader(f"Source Combination Options (Iteration {iteration + 1}/5)")

    for opt in options:
        with st.expander(f"Option {opt['option_letter']}: Score {opt['total_score']}/100"):
            st.write("**Score Breakdown:**")
            for criterion, data in opt['score_breakdown'].items():
                st.write(f"- {criterion}: {data['score']:.1f} ({data['weight']})")

            st.write("**Source Tables:**")
            for t in opt['tables']:
                st.write(f"- `{t['alias']}`: {t['full_name']} ({t['layer']})")

            st.write("**Join Conditions:**")
            for j in opt['joins']:
                st.write(f"- {j['source']} ↔ {j['target']}: `{j['condition']}`")

    selected = st.selectbox("Select an option:", ["Option A", "Option B", "Option C", "Reject All"])
    feedback = st.text_area("Feedback (optional):")

    if st.button("Submit"):
        # Return user decision
        pass
```

### Lineage Visualization (matplotlib/networkx)

```python
def render_lineage_visualization(lineage_data: Dict):
    import matplotlib.pyplot as plt
    import networkx as nx

    G = nx.DiGraph()

    for node in lineage_data["nodes"]:
        G.add_node(node["id"], label=node["label"], layer=node["layer"])

    for edge in lineage_data["edges"]:
        G.add_edge(edge["source"], edge["target"])

    # Color by layer
    colors = {"bronze": "#CD7F32", "silver": "#C0C0C0", "gold": "#FFD700", "target": "#4CAF50"}

    fig, ax = plt.subplots(figsize=(12, 8))
    pos = nx.spring_layout(G)
    nx.draw(G, pos, ax=ax, with_labels=True, node_size=2000, arrows=True)
    st.pyplot(fig)
```

---

## 15. Implementation Phases

### Phase 1: Foundation
- [ ] Update `AgentState` with new DDL fields
- [ ] Update intent classifier with `ddl_generation` and `pyspark_generation`
- [ ] Create `ddl_intent_parser_node`
- [ ] Update routing in `workflow.py`

### Phase 2: Discovery & Scoring
- [ ] Implement `multi_signal_discovery_node` (v3.0 - per-concept discovery)
- [ ] Implement edge case handlers (`_handle_no_table_concepts`, `_infer_concept_from_columns`)
- [ ] Implement routing function `route_discovery_result`
- [ ] Implement `CombinationScorer` class
- [ ] Implement `combination_scorer_node`
- [ ] Implement `recommendation_presenter_node`

### Phase 3: Generation & Validation
- [ ] Implement `ddl_generator_node` (SQL DDL)
- [ ] Implement `ddl_generator_node` (PySpark)
- [ ] Implement `ddl_validation_node`
- [ ] Implement `impact_analyzer_node`

### Phase 4: HITL & Execution
- [ ] Implement `concept_clarification_interrupt_node` (NEW in v3.0)
- [ ] Implement `option_selection_interrupt_node`
- [ ] Implement `code_approval_interrupt_node`
- [ ] Implement `execution_explainer_node`
- [ ] Implement `execution_approval_interrupt_node`
- [ ] Implement `ddl_executor_node`

### Phase 5: Ontology Sync
- [ ] Implement `ontology_sync_node`
- [ ] Implement `ontology_validation_node`
- [ ] Implement `format_ddl_result_node`

### Phase 6: Streamlit UI
- [ ] Add option selection UI
- [ ] Add lineage visualization (matplotlib/networkx)
- [ ] Add impact analysis display
- [ ] Add execution approval UI

### Phase 7: Testing & Integration
- [ ] End-to-end testing
- [ ] Edge case handling
- [ ] Error recovery
- [ ] Documentation

---

## 16. Test Scenarios

### Scenario 1: Create Gold Table
```
User: "Create a gold table for vendor purchase analysis"

Expected Flow:
1. Intent: ddl_generation, gold layer
2. Discover: e2e_sap_vend, e2e_sap_dn_po (silver)
3. Score: Option A (Silver+Silver), Option B (Silver+Bronze)
4. User selects Option A
5. Generate SQL DDL
6. Show impact: 10 columns, 2 source tables
7. User approves code
8. Show execution explanation
9. User approves execution
10. Execute on Databricks
11. Update Neo4j ontology
12. Validate ontology
```

### Scenario 2: Create Silver Table with Feedback
```
User: "Create a silver table with PO and material data"

Flow with rejection:
1. Intent: ddl_generation, silver layer
2. Options presented: A, B, C
3. User: "Reject - I need vendor data too"
4. Regenerate with feedback
5. New options include vendor tables
6. User selects Option B
7. Continue to generation...
```

### Scenario 3: PySpark Generation
```
User: "Write PySpark to create the vendor summary gold table"

Expected:
1. Intent: pyspark_generation, gold layer
2. Same discovery and scoring
3. Generate PySpark code instead of SQL
4. Same approval flow
```

### Scenario 4: Cancel Execution
```
User selects option, approves code, but cancels execution

Expected:
1. Return generated DDL/PySpark code
2. Do NOT execute on Databricks
3. Do NOT update Neo4j ontology
4. User can copy code for manual execution
```

---

## 17. Error Handling

### 17.1 Workflow-Level Errors

| Error Type | Handling |
|------------|----------|
| No viable combinations found | Show message, ask user to refine request |
| Max iterations (5) reached | Force selection of top option |
| Validation fails | Show errors, return to option selection |
| Execution fails | Show error, do NOT update ontology |
| Ontology sync fails | Alert user, suggest manual review |
| Ontology validation fails | Alert user, log warning |

### 17.2 Multi-Signal Discovery Edge Cases (NODE 2)

These edge cases are handled within `multi_signal_discovery_node`:

| Error Code | Scenario | Handling Strategy | Result |
|------------|----------|-------------------|--------|
| `NO_CONCEPTS_OR_COLUMNS` | LLM extracted no table_concepts AND no shared_columns | Return error with suggestion | User re-prompts |
| `CONCEPT_AMBIGUOUS` | Only shared_columns found, can't infer parent concept | Return clarification question | HITL asks user |
| `CONCEPT_NOT_FOUND` | A concept has no matching tables after signal fusion | Add warning, skip concept, continue | Partial results |
| `INVALID_TARGET_LAYER` | target_layer is not "silver" or "gold" | Return error with valid options | User re-prompts |
| `DUPLICATE_ANCHOR` | Same table is anchor for multiple concepts | Add info warning, continue normally | Normal operation |
| `MISSING_COLUMNS` | Some requested columns not found in path tables | Add warning, continue | Partial results |
| `NO_ANCHORS_FOUND` | All concepts failed signal matching | Return error | User re-prompts |
| `NO_PATHS_FOUND` | No graph paths connect the concept anchors | Return error with anchor list | User re-prompts |

### 17.3 Error Response Structure

All edge case errors follow a consistent structure:

```python
# Hard Error (stops processing)
{
    "error": "ERROR_CODE",
    "message": "Human-readable explanation",
    "suggestion": "What user can do to fix it",
    # Optional additional context
    "valid_options": ["silver", "gold"],  # For INVALID_TARGET_LAYER
    "concept_anchors": {...}              # For debugging
}

# Soft Error (continues with warning)
{
    "warnings": [
        {
            "type": "WARNING_TYPE",
            "message": "What happened",
            "concept": "affected_concept",  # Optional
            "columns_affected": [...]       # Optional
        }
    ],
    # ... normal response fields ...
}

# Clarification Request (HITL)
{
    "needs_clarification": True,
    "clarification_type": "CONCEPT_AMBIGUOUS",
    "question": "Which business area do these relate to?",
    "options": ["Purchase Orders", "Sales Orders", "Materials", ...]
}
```

### 17.4 Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                  multi_signal_discovery_node                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Validate target_layer ──────── INVALID? ──→ Return error    │
│                                        │                         │
│  2. Check table_concepts ──────── EMPTY? ───→ Try inference     │
│         │                                          │             │
│         │                          Confidence < 0.7? → Ask user  │
│         │                          Confidence ≥ 0.7? → Continue  │
│         ↓                                                        │
│  3. Per-concept discovery loop                                   │
│         │                                                        │
│         ├── No matches? ──→ Add warning, skip, continue         │
│         │                                                        │
│         ├── Tie? ──→ Secondary sort by path_length              │
│         │                                                        │
│         └── Success ──→ Add to concept_anchors                  │
│                                                                  │
│  4. Check duplicate anchors ──→ Add info warning if found       │
│                                                                  │
│  5. No anchors at all? ──────→ Return NO_ANCHORS_FOUND error    │
│                                                                  │
│  6. Graph pathfinding                                            │
│         │                                                        │
│         └── No paths? ──→ Return NO_PATHS_FOUND error           │
│                                                                  │
│  7. Check missing columns ──→ Add warning if found              │
│                                                                  │
│  8. Return results with warnings                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Success Criteria

1. User can generate DDL/PySpark for silver and gold tables
2. System presents 3 scored options with explanations
3. User can provide feedback up to 5 times
4. Lineage visualization shows data flow
5. Impact analysis explains what will change
6. Execution requires explicit approval
7. Neo4j ontology updated after successful execution
8. Ontology validation ensures no breaks
9. **Existing SQL generation flow remains unchanged**

---

## 18. Open Items / Future Enhancements

The following items were identified during the design review but deferred to future phases:

### P1 - High Priority (Next Sprint)

| Item | Node | Description |
|------|------|-------------|
| Layer fallback logic | NODE 1 | If user doesn't specify target layer, add fallback logic to either ask user or default to silver |

### P2 - Medium Priority (Future Enhancement)

| Item | Node | Description |
|------|------|-------------|
| ALTER support | NODE 10 | Implement `ALTER_ADD_COLUMN` and `ALTER_DROP_COLUMN` actions in ontology_sync_node. Currently only CREATE and DROP are supported. |
| Column selection UI | HITL Gates | Add UI for users to select/deselect specific columns before DDL generation. Currently all relevant columns are included automatically. |

### P3 - Low Priority (Nice to Have)

| Item | Node | Description |
|------|------|-------------|
| Undo capability | NODE 9/10 | Add ability to rollback execution - DROP the created table and remove ontology entries. Would require storing execution history. |
| Validation reset | Flow Logic | When validation fails and returns to recommendation_presenter, consider whether to reset `feedback_iteration` counter. |

### Design Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Scoring weights (v3.0) | 30/25/25/20 | Column Coverage (30%), Layer Priority (25%), Simplicity (25%), Pattern Match (20%) - prioritizes finding requested columns |
| Lineage inconsistencies | Warnings only | Don't block execution, just inform users of deviations from established patterns |
| Lineage relationship | IS_SOURCE_OF | Compatible with existing lineage tracking; direction is Source → Destination |
| LLM for concept extraction | Accepted | Using structured output (Pydantic) provides reasonable consistency; semantic search provides backup |

---

*End of Document*
