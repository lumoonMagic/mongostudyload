# Multi-Signal Table Discovery for DDL Generation

**Version**: 2.0
**Date**: December 18, 2025
**Status**: Design Document
**Author**: Architecture Team

> **Version 2.0 Update**: Enhanced with **Hierarchical Concept Extraction** based on latest
> text-to-SQL research (RSL-SQL, MAC-SQL, AmbiSQL). Column concepts are now nested under
> their parent table concepts, and signal discovery runs **per-concept** for guaranteed coverage.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Solution Overview](#2-solution-overview)
3. [Architecture](#3-architecture)
4. [Detailed Design](#4-detailed-design)
5. [Implementation Guide](#5-implementation-guide)
6. [Configuration](#6-configuration)
7. [Examples](#7-examples)
8. [FAQ](#8-faq)

---

## 1. Problem Statement

### 1.1 Current Situation

When a data engineer requests DDL generation:

```
"Create a Gold layer table E2EPOMATDETAILS containing PO line items
details along with material details, material group and deletion flag"
```

The system must identify which source tables to combine.

### 1.2 Current Approach (Problematic)

```
┌─────────────────────────────────────────────────────────────────────────┐
│ CURRENT FLOW                                                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Step 1: Extract concepts                                               │
│  ─────────────────────────                                              │
│  "PO line items" → requested_concepts[0]                                │
│  "material details" → requested_concepts[1]                             │
│                                                                         │
│  Step 2: Semantic search for EACH concept (top_k=5)                     │
│  ─────────────────────────────────────────────────                      │
│  "PO line items" → [ekko, ekpo, eket, ekkn, ekbe]     (5 tables)       │
│  "material details" → [mara, makt, marm, marc, mbew]  (5 tables)       │
│                                                                         │
│  Step 3: Cartesian Product                                              │
│  ─────────────────────────                                              │
│  5 × 5 = 25 combinations minimum                                        │
│  With 3 concepts: 5 × 5 × 5 = 125                                      │
│  With loose matching: 31,000 - 100,000 combinations                    │
│                                                                         │
│  Step 4: Validate joins AFTER generation                                │
│  ─────────────────────────────────────────                              │
│  For each of 100K combinations, check if joins exist                    │
│  Most combinations are INVALID → wasted compute                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Problems

| Problem | Impact |
|---------|--------|
| Vague concept extraction | "PO details" matches too many tables |
| Cartesian product explosion | O(n^k) combinations where n=tables per concept, k=concepts |
| Late join validation | Wasted compute on impossible combinations |
| No column awareness | Can't verify if tables have requested columns |
| No disambiguation | Proceeds even with unclear queries |

### 1.4 Goal

Replace Cartesian product enumeration with intelligent graph-based discovery:
- **Current**: 31,000 - 100,000 combinations
- **Target**: 5 - 20 valid paths

---

## 2. Solution Overview

### 2.1 Core Idea

Instead of generating all combinations and filtering, we:
1. **Find anchor tables** using multiple signals (column search, table search, lineage patterns)
2. **Traverse the graph** to find valid paths between anchors
3. **Score paths** by column coverage and return top recommendations

### 2.2 Key Principles

| Principle | Implementation |
|-----------|----------------|
| No hardcoded mappings | Use semantic search on existing descriptions |
| Multiple validation signals | Column + Table + Lineage signals fused |
| Graph-first, not enumerate-first | Path discovery via Neo4j traversal |
| Early filtering | Classify tables before path finding |
| Leverage existing data | Uses embeddings and IS_SOURCE_OF already in Neo4j |

### 2.3 High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│ MULTI-SIGNAL TABLE DISCOVERY v2.0 (Hierarchical)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐                                                       │
│  │ User Query   │                                                       │
│  └──────┬───────┘                                                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ STEP 1: Hierarchical Concept Extraction (Two-Pass)               │  │
│  │                                                                    │  │
│  │  PASS 1: Extract table_concepts (business entities)               │  │
│  │  ─────────────────────────────────────────────────                │  │
│  │  → "Purchase Order Line Item", "Material Details"                 │  │
│  │                                                                    │  │
│  │  PASS 2: For each table_concept, extract child column_concepts    │  │
│  │  ─────────────────────────────────────────────────────────────    │  │
│  │  "PO Line Item" → ["line item number", "price", "qty"]           │  │
│  │  "Material Details" → ["material group", "deletion flag"]         │  │
│  │                                                                    │  │
│  │  PASS 3: Identify shared_columns (ambiguous, no clear parent)     │  │
│  │  ─────────────────────────────────────────────────────────────    │  │
│  │  → ["description"] (could be PO or material description)          │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ STEP 2: Per-Concept Multi-Signal Discovery                        │  │
│  │                                                                    │  │
│  │  FOR EACH table_concept:                                          │  │
│  │  ┌────────────────────────────────────────────────────────────┐  │  │
│  │  │ "Purchase Order Line Item"                                  │  │  │
│  │  │   Column signal: search its children → ekpo (0.88)          │  │  │
│  │  │   Table signal: search concept name → ekpo (0.85)           │  │  │
│  │  │   Lineage signal: IS_SOURCE_OF → ekpo (0.90)                │  │  │
│  │  │   Fused: ekpo = 0.87 → ANCHOR for this concept              │  │  │
│  │  └────────────────────────────────────────────────────────────┘  │  │
│  │  ┌────────────────────────────────────────────────────────────┐  │  │
│  │  │ "Material Details"                                          │  │  │
│  │  │   Column signal: search its children → mara (0.91)          │  │  │
│  │  │   Table signal: search concept name → mara (0.82)           │  │  │
│  │  │   Lineage signal: IS_SOURCE_OF → mara (0.85)                │  │  │
│  │  │   Fused: mara = 0.86 → ANCHOR for this concept              │  │  │
│  │  └────────────────────────────────────────────────────────────┘  │  │
│  │                                                                    │  │
│  │  FOR shared_columns: Search across ALL concept anchors            │  │
│  │  → "description" found in mara.MAKTX → boost mara                 │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ STEP 3: Anchor Collection (One per Concept)                       │  │
│  │                                                                    │  │
│  │  concept_anchors = {                                              │  │
│  │    "PO Line Item": ekpo,                                          │  │
│  │    "Material Details": mara                                       │  │
│  │  }                                                                │  │
│  │  → Guarantees ALL user concepts are covered                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ STEP 4: Graph Path Discovery                                      │  │
│  │                                                                    │  │
│  │  Neo4j traversal: Find paths connecting ALL concept anchors       │  │
│  │  Query: MATCH path = (a)-[:RELATED_TO|JOINED_ON|...]*1..3]-(b)   │  │
│  │  WHERE a.name IN [anchors] AND b.name IN [anchors]               │  │
│  │                                                                    │  │
│  │  Result: Paths that connect tables from DIFFERENT concepts        │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ STEP 5: Scoring & Return                                          │  │
│  │                                                                    │  │
│  │  Score by: Coverage(30%) + Layer(25%) + Simplicity(25%) + Pattern(20%)│
│  │  Return: Top 3 recommendations with explanations                  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Architecture

### 3.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DDL WORKFLOW v2.0 (Hierarchical)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                 HierarchicalConceptExtractor                         │   │
│  │  ┌─────────────┐                                                    │   │
│  │  │   LLM       │  Two-Pass Extraction:                              │   │
│  │  │  (GPT-4o)   │                                                    │   │
│  │  │             │  PASS 1: Extract table_concepts                    │   │
│  │  │             │  PASS 2: For each, extract child column_concepts   │   │
│  │  │             │  PASS 3: Identify shared_columns (ambiguous)       │   │
│  │  └─────────────┘                                                    │   │
│  │                                                                      │   │
│  │  Output Structure:                                                   │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │ table_concepts: [                                            │   │   │
│  │  │   { concept_name: "PO Line Item",                            │   │   │
│  │  │     column_concepts: ["line item number", "price", "qty"] }, │   │   │
│  │  │   { concept_name: "Material Details",                        │   │   │
│  │  │     column_concepts: ["material group", "deletion flag"] }   │   │   │
│  │  │ ],                                                           │   │   │
│  │  │ shared_columns: ["description"]  // ambiguous                │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │              PerConceptMultiSignalDiscovery                          │   │
│  │                                                                      │   │
│  │   FOR EACH table_concept:                                           │   │
│  │   ┌─────────────────────────────────────────────────────────────┐   │   │
│  │   │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐   │   │   │
│  │   │  │ColumnSearcher │  │ TableSearcher │  │LineageSearcher│   │   │   │
│  │   │  │               │  │               │  │               │   │   │   │
│  │   │  │ Searches ONLY │  │ Searches      │  │ Queries       │   │   │   │
│  │   │  │ this concept's│  │ concept name  │  │ IS_SOURCE_OF  │   │   │   │
│  │   │  │ child columns │  │               │  │ patterns      │   │   │   │
│  │   │  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘   │   │   │
│  │   │          │                  │                  │            │   │   │
│  │   │          └──────────────────┼──────────────────┘            │   │   │
│  │   │                             ▼                               │   │   │
│  │   │                   ┌─────────────────┐                       │   │   │
│  │   │                   │ SignalFusion    │                       │   │   │
│  │   │                   │ (per concept)   │                       │   │   │
│  │   │                   │ col*0.35 +      │                       │   │   │
│  │   │                   │ tbl*0.30 +      │                       │   │   │
│  │   │                   │ lin*0.35        │                       │   │   │
│  │   │                   └────────┬────────┘                       │   │   │
│  │   │                            ▼                                │   │   │
│  │   │                   Best table = ANCHOR for this concept      │   │   │
│  │   └─────────────────────────────────────────────────────────────┘   │   │
│  │                                                                      │   │
│  │   FOR shared_columns: Search across ALL concept anchors              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                  ConceptAnchorCollector                              │   │
│  │                                                                      │   │
│  │   Input: Per-concept discovery results                              │   │
│  │   ┌─────────────────────────────────────────────────────────────┐   │   │
│  │   │ "PO Line Item" → {ekpo: 0.87, ekko: 0.65} → Best: ekpo      │   │   │
│  │   │ "Material Details" → {mara: 0.86, makt: 0.58} → Best: mara  │   │   │
│  │   └─────────────────────────────────────────────────────────────┘   │   │
│  │                                                                      │   │
│  │   Output: concept_anchors (one ANCHOR per concept)                  │   │
│  │   ┌─────────────────────────────────────────────────────────────┐   │   │
│  │   │ {                                                            │   │   │
│  │   │   "PO Line Item": {anchor: "ekpo", score: 0.87},            │   │   │
│  │   │   "Material Details": {anchor: "mara", score: 0.86}         │   │   │
│  │   │ }                                                            │   │   │
│  │   └─────────────────────────────────────────────────────────────┘   │   │
│  │   → Guarantees EVERY user concept is represented by a table         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    GraphPathDiscovery                                │   │
│  │                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────┐   │   │
│  │   │                     Neo4j Graph                              │   │   │
│  │   │                                                              │   │   │
│  │   │    (ekko)───RELATED_TO───(ekpo)───RELATED_TO───(mara)       │   │   │
│  │   │                          [PO]                   [MAT]        │   │   │
│  │   │                            │                     │          │   │   │
│  │   │                       RELATED_TO            RELATED_TO      │   │   │
│  │   │                            │                     │          │   │   │
│  │   │                            ▼                     ▼          │   │   │
│  │   │                         (eket)               (makt)         │   │   │
│  │   │                                                              │   │   │
│  │   └─────────────────────────────────────────────────────────────┘   │   │
│  │                                                                      │   │
│  │   Query: MATCH path = (a)-[:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER*1..3]-(b)│
│  │          WHERE a.name IN ['ekpo','mara'] AND b.name IN ['ekpo','mara']│  │
│  │                                                                      │   │
│  │   Result: Paths connecting anchors from DIFFERENT concepts          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      PathScorer                                      │   │
│  │                                                                      │   │
│  │   For each path, calculate:                                         │   │
│  │   • Column Coverage: Are concept's columns present? (30%)           │   │
│  │   • Layer Priority: Prefer Gold > Silver > Bronze (25%)             │   │
│  │   • Path Simplicity: Fewer joins = better (25%)                     │   │
│  │   • Pattern Match: IS_SOURCE_OF exists to target layer? (20%)       │   │
│  │                                                                      │   │
│  │   Return: Top 3 scored combinations                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA FLOW                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  User Query                                                                 │
│  "Create Gold table with PO line items, material group, deletion flag"      │
│       │                                                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ ConceptExtractor Output                                              │   │
│  │ {                                                                    │   │
│  │   "table_concepts": ["PO line items"],                              │   │
│  │   "column_concepts": ["material group", "deletion flag"],           │   │
│  │   "target_layer": "gold",                                           │   │
│  │   "action": "CREATE"                                                │   │
│  │ }                                                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ MultiSignalDiscovery Output                                          │   │
│  │                                                                      │   │
│  │ Signal 1 (Column Search):                                           │   │
│  │   "material group" ──► MATKL ──► mara (score: 0.92)                 │   │
│  │   "deletion flag"  ──► LVORM ──► mara (score: 0.89)                 │   │
│  │   Column signal: {mara: 0.90}                                       │   │
│  │                                                                      │   │
│  │ Signal 2 (Table Search):                                            │   │
│  │   "PO line items" ──► ekpo (score: 0.88)                            │   │
│  │   Table signal: {ekpo: 0.88, ekko: 0.72}                            │   │
│  │                                                                      │   │
│  │ Signal 3 (Lineage):                                                 │   │
│  │   Gold tables about PO/material sourced from:                       │   │
│  │   Lineage signal: {ekpo: 0.83, mara: 0.80, makt: 0.75}             │   │
│  │                                                                      │   │
│  │ FUSED: {ekpo: 0.85, mara: 0.82, makt: 0.45, ekko: 0.52, lfa1: 0.15}│   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Classification Output                                                │   │
│  │                                                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │ │ Table    │ Score │ Classification │ Reason                      │ │   │
│  │ ├──────────┼───────┼────────────────┼─────────────────────────────┤ │   │
│  │ │ ekpo     │ 0.85  │ ANCHOR         │ High table+lineage match    │ │   │
│  │ │ mara     │ 0.82  │ ANCHOR         │ High column+lineage match   │ │   │
│  │ │ ekko     │ 0.52  │ CANDIDATE      │ Moderate table match        │ │   │
│  │ │ makt     │ 0.45  │ CANDIDATE      │ Lineage pattern only        │ │   │
│  │ │ lfa1     │ 0.15  │ EXCLUDE        │ No relevant signals         │ │   │
│  │ └─────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ PathDiscovery Output                                                 │   │
│  │                                                                      │   │
│  │ Paths found between anchors [ekpo, mara]:                           │   │
│  │                                                                      │   │
│  │ Path 1: ekpo ──[MATNR]──► mara                                      │   │
│  │         Tables: [ekpo, mara]                                        │   │
│  │         Joins: [{from: ekpo, to: mara, on: MATNR}]                  │   │
│  │                                                                      │   │
│  │ Path 2: ekpo ──[MATNR]──► mara ──[MATNR]──► makt                    │   │
│  │         Tables: [ekpo, mara, makt]                                  │   │
│  │         Joins: [{from: ekpo, to: mara, on: MATNR},                  │   │
│  │                 {from: mara, to: makt, on: MATNR}]                  │   │
│  │                                                                      │   │
│  │ Path 3: ekko ──[EBELN]──► ekpo ──[MATNR]──► mara                    │   │
│  │         Tables: [ekko, ekpo, mara]                                  │   │
│  │         Joins: [{from: ekko, to: ekpo, on: EBELN},                  │   │
│  │                 {from: ekpo, to: mara, on: MATNR}]                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Final Scored Output                                                  │   │
│  │                                                                      │   │
│  │ ┌─────────────────────────────────────────────────────────────────┐ │   │
│  │ │ Rank │ Tables           │ Coverage │ Joins │ Score │ Status    │ │   │
│  │ ├──────┼──────────────────┼──────────┼───────┼───────┼───────────┤ │   │
│  │ │ 1    │ ekpo, mara       │ 100%     │ 1     │ 95    │ RECOMMEND │ │   │
│  │ │ 2    │ ekpo, mara, makt │ 100%+    │ 2     │ 90    │ RECOMMEND │ │   │
│  │ │ 3    │ ekko, ekpo, mara │ 100%     │ 2     │ 85    │ RECOMMEND │ │   │
│  │ └─────────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Detailed Design

### 4.1 Step 1: Hierarchical Concept Extraction (v2.0)

> **Research Basis**: Two-pass extraction inspired by RSL-SQL bidirectional approach,
> MAC-SQL decomposition, and AmbiSQL ambiguity handling.

**Purpose**: Extract table concepts with their nested column concepts, plus shared columns.

**Input**: Natural language query
**Output**: Hierarchical concepts with parent-child relationships

```python
class HierarchicalConceptExtractor:
    """
    Two-pass extraction that preserves column-to-concept relationships.

    Benefits over flat extraction:
    - Each concept gets its own anchor table (guaranteed coverage)
    - Columns boost only their parent concept's tables
    - Ambiguous columns handled separately
    """

    def extract(self, user_query: str) -> Dict:
        # PASS 1: Extract table concepts (business entities)
        table_concepts = self._extract_table_concepts(user_query)

        # PASS 2: For each table concept, extract child columns
        for concept in table_concepts:
            concept["column_concepts"] = self._extract_columns_for_concept(
                user_query, concept["concept_name"]
            )

        # PASS 3: Identify remaining columns (shared/ambiguous)
        all_assigned_columns = set()
        for concept in table_concepts:
            all_assigned_columns.update(concept["column_concepts"])

        shared_columns = self._extract_unassigned_columns(
            user_query, all_assigned_columns
        )

        return {
            "table_concepts": table_concepts,
            "shared_columns": shared_columns,
            "target_layer": self._extract_target_layer(user_query),
            "action": self._extract_action(user_query)
        }

    def _extract_table_concepts(self, user_query: str) -> List[Dict]:
        """PASS 1: Extract business entity concepts."""
        prompt = f"""
Analyze this DDL request and identify the BUSINESS ENTITIES (table concepts):

Query: "{user_query}"

A table concept is a business entity like:
- "Purchase Order", "Purchase Order Line Item"
- "Material", "Material Details"
- "Vendor", "Customer"
- "Sales Order", "Delivery"

Return JSON array:
[
  {{"concept_name": "...", "description": "..."}},
  {{"concept_name": "...", "description": "..."}}
]

Rules:
- Only include distinct business entities
- Do NOT include specific column/field names here
- Keep concept names business-friendly, not technical
"""
        result = self.llm.invoke(prompt)
        return json.loads(result)

    def _extract_columns_for_concept(
        self, user_query: str, concept_name: str
    ) -> List[Dict]:
        """PASS 2: Extract columns that belong to a specific concept."""
        prompt = f"""
Given this DDL request and a business concept, identify columns that belong to it:

Query: "{user_query}"
Business Concept: "{concept_name}"

Which specific fields/attributes from the query belong to "{concept_name}"?

Return JSON array of column concepts:
[
  {{"name": "...", "confidence": "high|medium|low"}},
  {{"name": "...", "confidence": "high|medium|low"}}
]

Rules:
- Only include columns that clearly belong to this concept
- Use "high" confidence when the column clearly relates to this concept
- Use "medium" if somewhat related
- Use "low" if uncertain - these may go to shared_columns
- If a column doesn't belong to this concept, don't include it
"""
        result = self.llm.invoke(prompt)
        return json.loads(result)

    def _extract_unassigned_columns(
        self, user_query: str, assigned_columns: Set[str]
    ) -> List[Dict]:
        """PASS 3: Find columns not assigned to any concept."""
        prompt = f"""
Given this DDL request, identify any remaining column/field references
that were not assigned to a specific business concept:

Query: "{user_query}"
Already Assigned: {list(assigned_columns)}

Return JSON array of unassigned columns:
[
  {{"name": "...", "reason": "ambiguous - could be X or Y"}}
]

These are columns that:
- Could belong to multiple concepts (e.g., "description")
- Don't clearly fit any identified business entity
- Are generic fields mentioned without context
"""
        result = self.llm.invoke(prompt)
        return json.loads(result)
```

**Example**:
```
Input: "Create Gold table with PO line items, material group, description, and deletion flag"

Output:
{
  "table_concepts": [
    {
      "concept_name": "Purchase Order Line Item",
      "description": "PO line item details",
      "column_concepts": [
        {"name": "line item number", "confidence": "high"},
        {"name": "price", "confidence": "high"},
        {"name": "quantity", "confidence": "high"}
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
    {"name": "description", "reason": "ambiguous - could be PO or material description"}
  ],
  "target_layer": "gold",
  "action": "CREATE"
}
```

**Why hierarchical extraction?**

| Flat Approach | Hierarchical Approach |
|--------------|----------------------|
| "material group" could boost wrong tables | "material group" only boosts Material concept tables |
| One anchor dominates, others missed | Each concept gets its own anchor |
| Can't explain which table serves which need | Clear: "ekpo for PO, mara for Material" |
| Ambiguous columns cause noise | Ambiguous columns handled separately |

---

### 4.2 Step 2: Per-Concept Multi-Signal Discovery (v2.0)

**Purpose**: Find the best table for EACH concept using multiple evidence sources.

> **Key Change in v2.0**: Signals run PER CONCEPT, not across all concepts together.
> This ensures each business entity gets its own anchor table.

```python
class PerConceptMultiSignalDiscovery:
    """
    Run multi-signal discovery for EACH table concept separately.

    Benefits:
    - Each concept gets dedicated signal discovery
    - One anchor per concept (guaranteed coverage)
    - Column signals only boost their parent concept
    """

    def discover(self, extracted_concepts: Dict, target_layer: str) -> Dict:
        concept_results = {}

        # FOR EACH table_concept, run all three signals
        for concept in extracted_concepts["table_concepts"]:
            concept_name = concept["concept_name"]
            child_columns = [c["name"] for c in concept.get("column_concepts", [])]

            # Run signals FOR THIS CONCEPT ONLY
            column_signal = self._search_by_columns(child_columns)
            table_signal = self._search_by_tables([concept_name])
            lineage_signal = self._search_by_lineage([concept_name], target_layer)

            # Fuse signals for this concept
            fused = self._fuse_signals(column_signal, table_signal, lineage_signal)

            # Select best table as ANCHOR for this concept
            if fused:
                best_table = max(fused.items(), key=lambda x: x[1])
                concept_results[concept_name] = {
                    "anchor": best_table[0],
                    "score": best_table[1],
                    "all_candidates": fused,
                    "column_concepts": child_columns
                }

        # Handle shared_columns: search across ALL anchors
        shared_column_results = self._resolve_shared_columns(
            extracted_concepts.get("shared_columns", []),
            [r["anchor"] for r in concept_results.values()]
        )

        return {
            "concept_anchors": concept_results,
            "shared_column_mapping": shared_column_results
        }

    def _resolve_shared_columns(
        self,
        shared_columns: List[Dict],
        anchor_tables: List[str]
    ) -> Dict:
        """Search shared columns across all concept anchors."""
        results = {}
        for col in shared_columns:
            col_name = col["name"]
            # Search in anchor tables only
            found_in = self._search_column_in_tables(col_name, anchor_tables)
            results[col_name] = found_in
        return results
```

**Example of Per-Concept Discovery**:
```
Input:
  table_concepts:
    - "Purchase Order Line Item" with columns ["line item number", "price"]
    - "Material Details" with columns ["material group", "deletion flag"]
  shared_columns: ["description"]

FOR "Purchase Order Line Item":
  Column signal (its children): {ekpo: 0.88, ekko: 0.45}
  Table signal: {ekpo: 0.85, eket: 0.60}
  Lineage signal: {ekpo: 0.90}
  Fused: {ekpo: 0.87, ekko: 0.35, eket: 0.30}
  → ANCHOR: ekpo (score: 0.87)

FOR "Material Details":
  Column signal (its children): {mara: 0.91, makt: 0.65}
  Table signal: {mara: 0.82, marc: 0.55}
  Lineage signal: {mara: 0.85}
  Fused: {mara: 0.86, makt: 0.45, marc: 0.35}
  → ANCHOR: mara (score: 0.86)

FOR shared_columns ["description"]:
  Search in anchors [ekpo, mara]
  → Found in mara.MAKTX (material description)

Output:
{
  "concept_anchors": {
    "Purchase Order Line Item": {"anchor": "ekpo", "score": 0.87},
    "Material Details": {"anchor": "mara", "score": 0.86}
  },
  "shared_column_mapping": {
    "description": {"table": "mara", "column": "MAKTX"}
  }
}
```

---

#### Individual Signal Functions (unchanged)

#### Signal 1: Column Semantic Search

Searches the `column_descriptions` vector index for column concepts.

```python
def search_by_columns(self, column_concepts: List[str]) -> Dict[str, float]:
    """
    Search column embeddings, return parent tables with scores.

    Why column search?
    - Columns are MORE SPECIFIC than tables
    - "material group" → MATKL → definitely in mara
    - Validates that the data actually exists
    """
    results = {}

    for concept in column_concepts:
        embedding = get_embedding(concept)

        cypher = """
        CALL db.index.vector.queryNodes('column_descriptions', 5, $embedding)
        YIELD node AS col, score
        MATCH (t:Table)-[:HAS_COLUMN]->(col)
        WHERE score > 0.7
        RETURN t.name AS table_name,
               t.full_name AS full_name,
               col.name AS column_name,
               col.description AS column_desc,
               score
        ORDER BY score DESC
        """

        matches = execute_cypher(cypher, {"embedding": embedding})

        for match in matches:
            table = match["table_name"]
            score = match["score"]
            # Keep highest score if table appears multiple times
            results[table] = max(results.get(table, 0), score)

    return results
```

**Example**:
```
Input: ["material group", "deletion flag"]

Column Search Results:
  "material group" → MATKL (mara) score=0.92
  "deletion flag"  → LVORM (mara) score=0.89

Output: {mara: 0.90}
```

#### Signal 2: Table Semantic Search

Searches the `table_descriptions` vector index for table concepts.

```python
def search_by_tables(self, table_concepts: List[str]) -> Dict[str, float]:
    """
    Search table embeddings directly.

    Why table search?
    - Catches tables that match the business entity
    - "PO line items" → ekpo (purchase order items table)
    """
    results = {}

    for concept in table_concepts:
        embedding = get_embedding(concept)

        cypher = """
        CALL db.index.vector.queryNodes('table_descriptions', 5, $embedding)
        YIELD node AS t, score
        WHERE score > 0.6
        RETURN t.name AS table_name,
               t.full_name AS full_name,
               t.description AS description,
               t.layer AS layer,
               score
        ORDER BY score DESC
        """

        matches = execute_cypher(cypher, {"embedding": embedding})

        for match in matches:
            table = match["table_name"]
            score = match["score"]
            results[table] = max(results.get(table, 0), score)

    return results
```

**Example**:
```
Input: ["PO line items"]

Table Search Results:
  "PO line items" → ekpo (0.88), ekko (0.72)

Output: {ekpo: 0.88, ekko: 0.72}
```

#### Signal 3: Lineage Pattern Search

Finds tables that historically feed similar tables in the target layer.

```python
def search_by_lineage(self, all_concepts: List[str], target_layer: str) -> Dict[str, float]:
    """
    Find tables that historically feed similar Gold/Silver tables.

    Why lineage search?
    - If Gold PO tables were built from ekpo+mara before, they're likely good sources
    - Learns from existing patterns without hardcoding
    - Unique advantage: uses IS_SOURCE_OF relationships
    """

    cypher = """
    // Find existing tables in target layer that match concepts
    MATCH (dst:Table {layer: $target_layer})
    WHERE any(concept IN $concepts WHERE
        toLower(dst.description) CONTAINS toLower(concept)
        OR toLower(dst.name) CONTAINS toLower(concept))

    // Find their sources via IS_SOURCE_OF
    MATCH (src:Table)-[:IS_SOURCE_OF*1..2]->(dst)

    // Return sources ranked by usage frequency
    RETURN src.name AS table_name,
           src.full_name AS full_name,
           count(DISTINCT dst) AS feeds_count,
           collect(DISTINCT dst.name)[0..3] AS sample_targets
    ORDER BY feeds_count DESC
    LIMIT 10
    """

    matches = execute_cypher(cypher, {
        "target_layer": target_layer,
        "concepts": all_concepts
    })

    # Normalize to 0-1 scale
    if not matches:
        return {}

    max_count = max(m["feeds_count"] for m in matches)
    return {
        m["table_name"]: m["feeds_count"] / max_count
        for m in matches
    }
```

**Example**:
```
Input: concepts=["PO", "material"], target_layer="gold"

Query: What tables feed existing Gold tables about PO/material?

Result:
  ekpo feeds 5 Gold tables → score: 1.0
  mara feeds 4 Gold tables → score: 0.8
  makt feeds 3 Gold tables → score: 0.6

Output: {ekpo: 1.0, mara: 0.8, makt: 0.6}
```

#### Signal Fusion

Combine all signals with configurable weights.

```python
def fuse_signals(
    self,
    column_signal: Dict[str, float],
    table_signal: Dict[str, float],
    lineage_signal: Dict[str, float]
) -> Dict[str, float]:
    """
    Weighted combination of all signals.

    Weights rationale:
    - Column (35%): Most specific signal, high confidence
    - Lineage (35%): Proven patterns, historical validation
    - Table (30%): Good but can be ambiguous
    """

    WEIGHTS = {
        "column": 0.35,
        "table": 0.30,
        "lineage": 0.35
    }

    # Get all candidate tables
    all_tables = (
        set(column_signal.keys()) |
        set(table_signal.keys()) |
        set(lineage_signal.keys())
    )

    combined = {}
    for table in all_tables:
        score = (
            column_signal.get(table, 0) * WEIGHTS["column"] +
            table_signal.get(table, 0) * WEIGHTS["table"] +
            lineage_signal.get(table, 0) * WEIGHTS["lineage"]
        )
        combined[table] = round(score, 3)

    return dict(sorted(combined.items(), key=lambda x: -x[1]))
```

**Example**:
```
Column Signal:  {mara: 0.90}
Table Signal:   {ekpo: 0.88, ekko: 0.72}
Lineage Signal: {ekpo: 1.0, mara: 0.8, makt: 0.6}

Fusion Calculation:
  ekpo: (0 * 0.35) + (0.88 * 0.30) + (1.0 * 0.35) = 0.614
  mara: (0.90 * 0.35) + (0 * 0.30) + (0.8 * 0.35) = 0.595
  makt: (0 * 0.35) + (0 * 0.30) + (0.6 * 0.35) = 0.210
  ekko: (0 * 0.35) + (0.72 * 0.30) + (0 * 0.35) = 0.216

Output: {ekpo: 0.614, mara: 0.595, ekko: 0.216, makt: 0.210}
```

---

### 4.3 Step 3: Concept Anchor Collection (v2.0)

> **Key Change in v2.0**: Instead of global classification with thresholds, we now
> collect ONE anchor per concept. This guarantees all user concepts are covered.

**Purpose**: Collect the best table (anchor) for each concept.

```python
def collect_concept_anchors(self, concept_discovery_results: Dict) -> Dict:
    """
    Collect anchors from per-concept discovery.

    v2.0 approach: Each concept gets its own anchor (the best table for that concept).
    This replaces global thresholding which could miss concepts.

    OLD approach (v1.0):
      Global scores → threshold → some tables become anchors
      Problem: If user asks for 3 concepts but 1 table dominates, only 1 anchor

    NEW approach (v2.0):
      Per-concept scores → best table per concept = anchor
      Guarantee: Every concept has an anchor table
    """

    concept_anchors = concept_discovery_results["concept_anchors"]

    # Collect all anchors (one per concept)
    anchors = []
    for concept_name, result in concept_anchors.items():
        anchors.append({
            "table": result["anchor"],
            "score": result["score"],
            "for_concept": concept_name,
            "column_concepts": result.get("column_concepts", [])
        })

    # Collect candidates (other high-scoring tables from any concept)
    candidates = []
    for concept_name, result in concept_anchors.items():
        for table, score in result.get("all_candidates", {}).items():
            if table != result["anchor"] and score >= 0.2:
                candidates.append({
                    "table": table,
                    "score": score,
                    "related_concept": concept_name
                })

    return {
        "anchors": anchors,
        "candidates": candidates,
        "anchor_tables": [a["table"] for a in anchors]
    }
```

**Example**:
```
Input (from per-concept discovery):
{
  "concept_anchors": {
    "Purchase Order Line Item": {"anchor": "ekpo", "score": 0.87},
    "Material Details": {"anchor": "mara", "score": 0.86}
  }
}

Output:
{
  "anchors": [
    {"table": "ekpo", "score": 0.87, "for_concept": "Purchase Order Line Item"},
    {"table": "mara", "score": 0.86, "for_concept": "Material Details"}
  ],
  "candidates": [
    {"table": "ekko", "score": 0.35, "related_concept": "Purchase Order Line Item"},
    {"table": "makt", "score": 0.45, "related_concept": "Material Details"}
  ],
  "anchor_tables": ["ekpo", "mara"]
}
```

**Why concept-based anchors are better**:

| Old Global Classification | New Per-Concept Anchors |
|--------------------------|-------------------------|
| One table dominates, others missed | Each concept guaranteed an anchor |
| Can't explain why tables were chosen | Clear: "ekpo for PO, mara for Material" |
| Threshold-based (arbitrary cutoffs) | Best-per-concept (always finds something) |
| User asks for 3 concepts, might get 1 | User asks for 3 concepts, gets 3 anchors |

---

### 4.4 Step 4: Graph Path Discovery

**Purpose**: Find valid paths between anchor tables using Neo4j graph traversal.

**This is the key innovation that replaces Cartesian product.**

```python
def find_paths(
    self,
    anchors: List[str],
    candidates: List[str],
    max_depth: int = 3
) -> List[Dict]:
    """
    Find all valid paths connecting anchor tables.

    Key insight: Instead of generating all combinations and checking joins,
    we traverse the graph to find ONLY connected paths.

    This is O(paths) instead of O(n^k) Cartesian product.
    """

    if len(anchors) < 2:
        # Single anchor - just return it
        return [{"tables": anchors, "joins": [], "path_length": 1}]

    anchor_names = [a["table"] for a in anchors]

    cypher = """
    // Find paths connecting anchor tables using all join relationship types
    MATCH path = (start:Table)-[:RELATED_TO|JOINED_ON|LEFT_OUTER|INNER*1..3]-(end:Table)
    WHERE start.name IN $anchors
      AND end.name IN $anchors
      AND start <> end

    // Extract path details
    WITH path,
         [node IN nodes(path) | node.name] AS table_names,
         [node IN nodes(path) | node.full_name] AS table_full_names,
         [rel IN relationships(path) | {
           from_table: startNode(rel).name,
           to_table: endNode(rel).name,
           join_column: rel.src_column,
           join_condition: rel.join_condition
         }] AS joins

    // Deduplicate (path A->B->C same as C->B->A)
    WITH table_names, table_full_names, joins,
         reduce(sig = '', t IN table_names | sig + t) AS path_signature

    RETURN DISTINCT
        table_names AS tables,
        table_full_names AS full_names,
        joins,
        size(table_names) AS path_length
    ORDER BY path_length ASC
    LIMIT 20
    """

    paths = execute_cypher(cypher, {"anchors": anchor_names})

    # Enrich paths with metadata
    results = []
    for path in paths:
        results.append({
            "tables": path["tables"],
            "full_names": path["full_names"],
            "joins": path["joins"],
            "path_length": path["path_length"]
        })

    return results
```

**Example**:
```
Anchors: [ekpo, mara]

Neo4j Traversal Query:
  Find all paths between ekpo and mara via RELATED_TO

Paths Found:
  Path 1: ekpo ─[MATNR]─► mara          (length: 2)
  Path 2: ekpo ─[EBELN]─► ekko ─...     (length: 3, via header)

Output: Only valid, connected paths - NOT 100K combinations
```

---

### 4.5 Step 5: Path Scoring

**Purpose**: Score each path by column coverage, layer priority, simplicity, and pattern match.

```python
LAYER_SCORES = {"gold": 100, "silver": 75, "bronze": 50}

def score_paths(
    self,
    paths: List[Dict],
    required_columns: List[str],
    target_layer: str
) -> List[Dict]:
    """
    Score each path and rank them.

    Scoring factors:
    - Column Coverage (30%): Do requested columns exist?
    - Layer Priority (25%): Prefer higher maturity data (Gold > Silver > Bronze)
    - Path Simplicity (25%): Fewer joins = simpler = better
    - Pattern Match (20%): Does IS_SOURCE_OF exist for this combo?
    """

    scored = []

    for path in paths:
        tables = path["tables"]
        layers = path.get("layers", [])

        # Calculate column coverage
        coverage = self._calculate_column_coverage(tables, required_columns)

        # Calculate layer priority
        if layers:
            layer_score = sum(LAYER_SCORES.get(l, 50) for l in layers) / len(layers) / 100
        else:
            layer_score = 0.5

        # Calculate simplicity (inverse of path length)
        max_length = 5
        simplicity = max(0, (max_length - path["path_length"]) / max_length)

        # Check pattern match
        pattern_score = self._check_lineage_pattern(tables, target_layer)

        # Weighted total
        total = (
            coverage["score"] * 0.30 +
            layer_score * 0.25 +
            simplicity * 0.25 +
            pattern_score * 0.20
        )

        scored.append({
            **path,
            "column_coverage": coverage,
            "layer_score": layer_score,
            "simplicity_score": simplicity,
            "pattern_score": pattern_score,
            "total_score": round(total * 100, 1)
        })

    # Sort by total score descending
    scored.sort(key=lambda x: -x["total_score"])

    return scored[:3]  # Return top 3

def _calculate_column_coverage(
    self,
    tables: List[str],
    required_columns: List[str]
) -> Dict:
    """Check what percentage of required columns exist in path tables."""

    if not required_columns:
        return {"score": 1.0, "covered": [], "missing": []}

    # Get all columns in these tables
    cypher = """
    MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
    WHERE t.name IN $tables
    RETURN c.name AS column_name,
           c.description AS column_desc,
           t.name AS table_name
    """

    all_columns = execute_cypher(cypher, {"tables": tables})

    # Build searchable text
    column_info = [
        f"{c['column_name']} {c['column_desc'] or ''}"
        for c in all_columns
    ]

    covered = []
    missing = []

    for req in required_columns:
        found = any(
            req.lower() in col.lower()
            for col in column_info
        )
        if found:
            covered.append(req)
        else:
            missing.append(req)

    return {
        "score": len(covered) / len(required_columns),
        "covered": covered,
        "missing": missing
    }
```

---

## 4.6 Edge Cases & Error Handling

> **Research Basis**: Error handling strategies informed by [AmbiSQL](https://arxiv.org/html/2508.15276),
> [Sphinteract](https://dl.acm.org/doi/10.14778/3717755.3717772), and [RSL-SQL](https://arxiv.org/html/2411.00073v1).

### Edge Case 1: No table_concepts Extracted

**Scenario**: User query contains only columns, no business entities.
```
Query: "Create table with price, quantity, and description"
Result: table_concepts = [], shared_columns = ["price", "quantity", "description"]
```

**Handling Strategy**: Infer → Ask

```python
def _handle_no_table_concepts(extracted_concepts: Dict, user_query: str) -> Dict:
    """
    Handle case where LLM extracted no table_concepts.
    Strategy: Try to infer from columns, then ask user if still ambiguous.
    """
    table_concepts = extracted_concepts.get("table_concepts", [])
    shared_columns = extracted_concepts.get("shared_columns", [])

    if table_concepts:
        return extracted_concepts  # Normal case, no handling needed

    if not shared_columns:
        # No concepts AND no columns - cannot proceed
        return {
            "error": "NO_CONCEPTS_OR_COLUMNS",
            "message": "Could not identify any business concepts or columns from query.",
            "suggestion": "Please specify what data you need (e.g., 'PO details with material group')"
        }

    # Try to infer table_concept from columns
    inferred = _infer_concept_from_columns(shared_columns, user_query)

    if inferred["confidence"] >= 0.7:
        # High confidence inference - create synthetic concept
        return {
            "table_concepts": [{
                "concept_name": inferred["concept_name"],
                "description": inferred["description"],
                "column_concepts": [{"name": c, "confidence": "inferred"} for c in shared_columns]
            }],
            "shared_columns": [],
            "inferred": True,
            "inference_confidence": inferred["confidence"]
        }
    else:
        # Low confidence - ask user for clarification
        return {
            "needs_clarification": True,
            "clarification_type": "CONCEPT_AMBIGUOUS",
            "message": "I found columns but couldn't identify the business context.",
            "columns_found": shared_columns,
            "suggested_concepts": inferred.get("suggestions", []),
            "question": "Which business area do these columns relate to?",
            "options": ["Purchase Orders", "Sales Orders", "Materials", "Vendors", "Other"]
        }


def _infer_concept_from_columns(columns: List[str], user_query: str) -> Dict:
    """
    LLM second pass to infer business concept from columns.
    """
    prompt = f"""
Given these columns: {columns}
From query: "{user_query}"

What business entity/concept do these columns most likely belong to?

Return JSON:
{{
  "concept_name": "...",
  "description": "...",
  "confidence": 0.0-1.0,
  "suggestions": ["Alternative1", "Alternative2"]
}}
"""
    return llm.invoke(prompt)


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
```

**Important**: When the workflow re-enters `multi_signal_discovery_node` after user clarification,
check for `user_clarification_response` in state BEFORE calling `_handle_no_table_concepts`:

```python
# In multi_signal_discovery_node, before edge case handling:
user_clarification = state.get("user_clarification_response")
if not table_concepts and user_clarification:
    # User already provided clarification - build concept from it
    table_concepts = [_build_concept_from_clarification(
        user_clarification, shared_columns, user_query
    )]
```

---

### Edge Case 2: Concept with No Signal Matches

**Scenario**: User mentions a concept that doesn't exist in the ontology.
```
Query: "Create table with spacecraft telemetry data"
Signals: column_signal={}, table_signal={}, lineage_signal={}
```

**Handling Strategy**: Continue with partial results + warning

```python
def _handle_concept_discovery(concept: Dict, allowed_layers: List[str],
                               target_layer: str) -> Tuple[Dict, Optional[Dict]]:
    """
    Run signal discovery for a single concept.
    Returns: (result_dict, warning_dict or None)
    """
    concept_name = concept["concept_name"]
    child_columns = [c["name"] for c in concept.get("column_concepts", [])]

    # Run all signals
    column_signal = _search_by_columns(child_columns, allowed_layers)
    table_signal = _search_by_tables([concept_name], allowed_layers)
    lineage_signal = _search_by_lineage([concept_name], target_layer, allowed_layers)

    # Fuse signals
    fused = _fuse_signals(column_signal, table_signal, lineage_signal)

    if not fused:
        # No matches found for this concept
        warning = {
            "type": "CONCEPT_NOT_FOUND",
            "concept": concept_name,
            "message": f"No matching tables found for '{concept_name}'",
            "suggestion": "Consider rephrasing or verify this data exists in the system",
            "searched_columns": child_columns,
            "searched_layers": allowed_layers
        }
        return None, warning

    # Select best table as anchor
    best_table = max(fused.items(), key=lambda x: x[1])
    result = {
        "anchor": best_table[0],
        "score": best_table[1],
        "all_candidates": fused,
        "column_concepts": child_columns
    }

    return result, None
```

**Output Structure with Warnings**:
```python
{
    "concept_anchors": {
        "Purchase Order Line Item": {"anchor": "ekpo", "score": 0.87}
        # "Spacecraft Telemetry" NOT included - no match
    },
    "warnings": [
        {
            "type": "CONCEPT_NOT_FOUND",
            "concept": "Spacecraft Telemetry",
            "message": "No matching tables found for 'Spacecraft Telemetry'",
            "suggestion": "Consider rephrasing or verify this data exists"
        }
    ],
    "partial_coverage": True,  # Not all concepts found
    "discovered_paths": [...]   # Paths from successful concepts only
}
```

---

### Edge Case 3: Invalid target_layer

**Scenario**: target_layer is neither "silver" nor "gold".

**Handling Strategy**: Validate at entry point

```python
VALID_LAYERS = ["silver", "gold"]

def multi_signal_discovery_node(state: AgentState) -> Dict:
    target_layer = state.get("target_layer")

    # Validate target_layer
    if not target_layer or target_layer.lower() not in VALID_LAYERS:
        return {
            "error": "INVALID_TARGET_LAYER",
            "message": f"Invalid target_layer: '{target_layer}'",
            "valid_options": VALID_LAYERS,
            "suggestion": "Specify 'silver' or 'gold' as the target layer"
        }

    target_layer = target_layer.lower()  # Normalize
    # ... continue with normal flow
```

---

### Edge Case 4: Same Table is Anchor for Multiple Concepts

**Scenario**: Two concepts both map to the same table.
```
Query: "Create table with PO Header details and PO Line Item details"
Result: "PO Header" → ekpo, "PO Line Item" → ekpo
```

**Handling Strategy**: Allow + inform user

```python
def _collect_anchors_with_dedup_info(concept_anchors: Dict) -> Dict:
    """
    Collect anchors and detect if same table serves multiple concepts.
    """
    anchors = []
    table_to_concepts = {}

    for concept_name, result in concept_anchors.items():
        table = result["anchor"]
        anchors.append({
            "table": table,
            "score": result["score"],
            "for_concept": concept_name,
            "column_concepts": result.get("column_concepts", [])
        })

        # Track which concepts map to which tables
        if table not in table_to_concepts:
            table_to_concepts[table] = []
        table_to_concepts[table].append(concept_name)

    # Detect duplicates
    info_messages = []
    for table, concepts in table_to_concepts.items():
        if len(concepts) > 1:
            info_messages.append({
                "type": "MERGED_CONCEPTS",
                "table": table,
                "concepts": concepts,
                "message": f"Table '{table}' serves multiple concepts: {concepts}",
                "suggestion": "These concepts may be redundant or closely related"
            })

    return {
        "anchors": anchors,
        "anchor_tables": list(set(a["table"] for a in anchors)),  # Deduplicated
        "info": info_messages
    }
```

---

### Edge Case 5: Concept's Columns Not in Anchor Table

**Scenario**: User assigns columns to wrong concept, and anchor doesn't have them.
```
User assigns "material group" to "PO Line Item"
ekpo (anchor for PO Line Item) doesn't have material group - it's in mara
```

**Handling Strategy**: Early warning (final coverage handled by path scorer)

```python
def _check_columns_in_anchor(concept_name: str, anchor_table: str,
                              column_concepts: List[str]) -> Optional[Dict]:
    """
    Check if requested columns exist in the anchor table.
    Returns warning if columns are missing.
    """
    # Get anchor's columns
    anchor_columns = execute_cypher("""
        MATCH (t:Table {name: $table})-[:HAS_COLUMN]->(c:Column)
        RETURN c.name AS name, c.description AS description
    """, {"table": anchor_table})

    anchor_col_names = [c["name"].lower() for c in anchor_columns]
    anchor_col_descs = [c["description"].lower() for c in anchor_columns if c["description"]]

    # Check which columns might be missing
    potentially_missing = []
    for col in column_concepts:
        col_lower = col.lower()
        found = any(col_lower in name or col_lower in desc
                   for name in anchor_col_names
                   for desc in anchor_col_descs)
        if not found:
            potentially_missing.append(col)

    if potentially_missing:
        return {
            "type": "COLUMNS_NOT_IN_ANCHOR",
            "concept": concept_name,
            "anchor": anchor_table,
            "missing_columns": potentially_missing,
            "message": f"Columns {potentially_missing} may not exist in '{anchor_table}'",
            "suggestion": "These columns may come from joined tables in the final path"
        }

    return None
```

---

### Edge Case 6: Tie-Breaking for Identical Scores

**Scenario**: Two paths have identical total scores.

**Handling Strategy**: Secondary sort by path_length (simpler wins)

```python
def score_paths(self, paths: List[Dict], ...) -> List[Dict]:
    # ... scoring logic ...

    # Sort by score DESC, then by path_length ASC (simpler paths win ties)
    scored.sort(key=lambda x: (-x["total_score"], x["path_length"]))

    return scored[:3]
```

---

### Summary: Error Response Structure

All error handling follows a consistent response structure:

```python
{
    # Success fields (when applicable)
    "concept_anchors": {...},
    "discovered_paths": [...],

    # Error fields (when applicable)
    "error": "ERROR_CODE",          # Machine-readable error code
    "message": "Human message",      # User-friendly description
    "suggestion": "What to do",      # Actionable guidance

    # Warning fields (partial success)
    "warnings": [...],               # Non-fatal issues
    "info": [...],                   # Informational messages
    "partial_coverage": True/False,  # Whether all concepts were found

    # Clarification request (when user input needed)
    "needs_clarification": True,
    "clarification_type": "...",
    "question": "...",
    "options": [...]
}
```

---

## 5. Implementation Guide

### 5.1 Prerequisites

Ensure Neo4j has:
- Vector index `table_descriptions` on Table.description_embedding
- Vector index `column_descriptions` on Column.description_embedding
- Relationships: HAS_COLUMN, RELATED_TO, IS_SOURCE_OF
- Embeddings generated for all tables and columns

### 5.2 New State Fields (v2.0 Updated)

Add to `AgentState`:

```python
# Hierarchical concept extraction (v2.0)
extracted_concepts: Optional[Dict]
"""
{
  'table_concepts': [
    {
      'concept_name': 'Purchase Order Line Item',
      'description': 'PO line item details',
      'column_concepts': [
        {'name': 'line item number', 'confidence': 'high'},
        {'name': 'price', 'confidence': 'high'}
      ]
    }
  ],
  'shared_columns': [
    {'name': 'description', 'reason': 'ambiguous - could be PO or material'}
  ],
  'target_layer': str,
  'action': str
}
"""

# Per-concept anchor results (v2.0)
concept_anchors: Optional[Dict]
"""
{
  'Purchase Order Line Item': {'anchor': 'ekpo', 'score': 0.87, 'column_concepts': [...]},
  'Material Details': {'anchor': 'mara', 'score': 0.86, 'column_concepts': [...]}
}
"""

# Shared column resolution (v2.0)
shared_column_mapping: Optional[Dict]
"""
{
  'description': {'table': 'mara', 'column': 'MAKTX', 'score': 0.78}
}
"""

# Classification results (v2.0 - concept-based)
table_classification: Optional[Dict]
"""
{
  'anchors': [{'table': 'ekpo', 'score': 0.87, 'for_concept': 'PO Line Item'}],
  'anchor_tables': ['ekpo', 'mara']
}
"""

# Path discovery results
discovered_paths: Optional[List[Dict]]
"""Valid paths from graph traversal with concepts_covered"""

# ============================================
# Discovery Warnings & Errors (v3.0 NEW)
# ============================================

discovery_warnings: Optional[List[Dict]]
"""
Non-fatal warnings from multi-signal discovery:
[
  {'type': 'CONCEPT_NOT_FOUND', 'concept': '...', 'message': '...'},
  {'type': 'DUPLICATE_ANCHOR', 'anchor': '...', 'concepts': [...]},
  {'type': 'MISSING_COLUMNS', 'path': [...], 'missing': [...]}
]
"""

discovery_error: Optional[Dict]
"""
Fatal error from multi-signal discovery:
{
  'error': 'NO_ANCHORS_FOUND',  # or INVALID_TARGET_LAYER, NO_PATHS_FOUND, NO_CONCEPTS_OR_COLUMNS
  'message': 'Human-readable explanation',
  'suggestion': 'What user can do'
}
"""

needs_clarification: Optional[Dict]
"""
Clarification request (triggers HITL concept_clarification_gate):
{
  'needs_clarification': True,
  'clarification_type': 'CONCEPT_AMBIGUOUS',
  'question': 'Which business area do these relate to?',
  'options': [...]
}
"""

user_clarification_response: Optional[str]
"""User's response to clarification question"""
```

### 5.3 Implementation Order (v2.0 Updated)

**Phase 1**: Hierarchical Concept Extraction
1. Implement `HierarchicalConceptExtractor` with two-pass extraction
2. Implement confidence scoring for column assignments
3. Implement shared_columns detection

**Phase 2**: Per-Concept Signal Discovery
1. Implement `PerConceptMultiSignalDiscovery`
2. Implement per-concept `ColumnSearcher`, `TableSearcher`, `LineageSearcher`
3. Implement per-concept `SignalFusion`
4. Implement `ConceptAnchorCollector`

**Phase 3**: Graph Path Discovery
1. Implement `GraphPathDiscovery`
2. Implement `_resolve_shared_columns`
3. Implement `PathScorer` with concept-aware scoring
4. Update `multi_signal_discovery_node`

**Phase 4**: Robustness
1. Add edge case handling (no anchors, disconnected anchors, etc.)
2. Add retry logic with relaxed thresholds
3. Add detailed logging and explainability

---

## 6. Configuration

```python
# config.py additions

MULTI_SIGNAL_CONFIG = {
    # Signal weights (must sum to 1.0)
    "weights": {
        "column": 0.35,
        "table": 0.30,
        "lineage": 0.35
    },

    # Classification thresholds
    "thresholds": {
        "anchor": 0.7,      # Score >= 0.7 = ANCHOR
        "candidate": 0.2    # Score >= 0.2 = CANDIDATE, else EXCLUDE
    },

    # Search parameters
    "search": {
        "column_min_score": 0.7,   # Min score for column matches
        "table_min_score": 0.6,    # Min score for table matches
        "top_k": 5,                # Results per search
        "max_path_depth": 3,       # Max joins in path
        "max_paths": 20            # Max paths to return
    },

    # Scoring weights (Level 2: Path Scoring)
    "scoring": {
        "column_coverage": 0.30,
        "layer_priority": 0.25,
        "simplicity": 0.25,
        "pattern_match": 0.20
    }
}
```

---

## 7. Examples

### 7.1 Simple Query

**Input**:
```
"Create Gold table with vendor name and vendor city"
```

**Step 1 - Concepts**:
```json
{
  "table_concepts": ["vendor"],
  "column_concepts": ["vendor name", "vendor city"],
  "target_layer": "gold"
}
```

**Step 2 - Signals**:
```
Column: "vendor name" → NAME1 → lfa1 (0.91)
        "vendor city" → ORT01 → lfa1 (0.88)
Table:  "vendor" → lfa1 (0.85)
Lineage: lfa1 feeds Gold vendor tables (0.90)

Fused: {lfa1: 0.88}
```

**Step 3 - Classification**:
```
Anchors: [lfa1]
Candidates: []
Excluded: [ekko, mara, ...]
```

**Step 4 - Paths**:
```
Single anchor - no path needed
Result: [{tables: [lfa1], joins: []}]
```

**Output**: Recommend lfa1 as single source table.

---

### 7.2 Multi-Table Query

**Input**:
```
"Create Gold table E2EPOMATDETAILS with PO line items,
material description, material group and deletion flag"
```

**Step 1 - Concepts**:
```json
{
  "table_concepts": ["PO line items"],
  "column_concepts": ["material description", "material group", "deletion flag"],
  "target_layer": "gold"
}
```

**Step 2 - Signals**:
```
Column: "material description" → MAKTX → makt (0.93)
        "material group" → MATKL → mara (0.91)
        "deletion flag" → LVORM → mara (0.88)
Table:  "PO line items" → ekpo (0.89)
Lineage: ekpo (0.85), mara (0.80), makt (0.75)

Fused: {ekpo: 0.72, mara: 0.71, makt: 0.58}
```

**Step 3 - Classification**:
```
Anchors: [ekpo, mara] (both ≥ 0.7)
Candidates: [makt] (0.58)
Excluded: [lfa1, kna1, ...]
```

**Step 4 - Paths**:
```
Neo4j traversal between [ekpo, mara]:

Path 1: ekpo ─[MATNR]─► mara
        Coverage: material group ✓, deletion flag ✓, material description ✗
        Score: 85

Path 2: ekpo ─[MATNR]─► mara ─[MATNR]─► makt
        Coverage: material group ✓, deletion flag ✓, material description ✓
        Score: 95
```

**Output**:
```
Recommendation 1: ekpo + mara + makt (Score: 95)
  - Full column coverage
  - 2 joins required
  - Follows existing lineage pattern

Recommendation 2: ekpo + mara (Score: 85)
  - Missing: material description
  - 1 join required
  - Simpler structure
```

---

### 7.3 Ambiguous Query

**Input**:
```
"Create a table with order data"
```

**Step 1 - Concepts**:
```json
{
  "table_concepts": ["order data"],
  "column_concepts": [],
  "target_layer": "gold"
}
```

**Step 2 - Signals**:
```
Column: (none - no column concepts)
Table:  "order data" → vbak (0.75), ekko (0.73), ...
Lineage: Multiple matches

Fused: {vbak: 0.52, ekko: 0.51, ekpo: 0.48, ...}
```

**Step 3 - Classification**:
```
Anchors: [] (none ≥ 0.7)
Candidates: [vbak, ekko, ekpo, ...]
Excluded: []
```

**Confidence Check FAILS**: No anchors found.

**Action**: Ask user for clarification:
```
"Your query 'order data' is ambiguous. Did you mean:
- Purchase Orders (ekko, ekpo)
- Sales Orders (vbak, vbap)
- Production Orders (aufk)

Please specify which type of order."
```

---

## 8. FAQ

**Q: What if semantic search returns wrong tables?**

A: The multi-signal approach mitigates this. Even if table search is wrong, column search and lineage patterns provide correction. All three signals must align for high confidence.

---

**Q: How does this handle new tables added to Neo4j?**

A: Automatically. New tables with embeddings are discoverable via semantic search. No code changes required - just ensure embeddings are generated when tables are added.

---

**Q: What if required columns span many tables?**

A: The path discovery will find longer paths (up to `max_path_depth`). If columns can't be covered in one connected path, the system returns partial matches with warnings about missing columns.

---

**Q: Can I adjust the signal weights?**

A: Yes. Weights are configurable in `MULTI_SIGNAL_CONFIG`. If column descriptions are poor quality, reduce column weight. If lineage data is sparse, reduce lineage weight.

---

**Q: What's the performance improvement?**

A:
- Current: O(n^k) where n=tables per concept, k=concepts → 31K-100K combinations
- New: O(anchors × path_depth) → typically 5-20 paths

For 3 concepts with 5 tables each:
- Current: 5³ = 125 combinations (best case), often 31K+
- New: ~2 anchors, depth 3 → ~10 paths

---

**Q: What if IS_SOURCE_OF patterns don't exist yet?**

A: The system still works. Lineage signal returns 0, and classification relies on column+table signals. As more tables are created and lineage grows, recommendations improve automatically.

---

**Q: How do I debug poor recommendations?**

A: Check each signal individually:
1. Column search: Are column descriptions accurate?
2. Table search: Are table descriptions specific enough?
3. Lineage: Does IS_SOURCE_OF data exist for this domain?
4. Paths: Do RELATED_TO relationships connect the tables?

---

## Appendix: Research References

This design incorporates techniques from:

| Technique | Source | How Used |
|-----------|--------|----------|
| Bidirectional linking | RSL-SQL (2024) | Column search + Table search |
| Graph pathfinding | SchemaGraphSQL (2025) | Neo4j path traversal |
| Question enrichment | E-SQL (2024) | Concept extraction approach |
| Table classification | MAC-SQL (2025) | ANCHOR/CANDIDATE/EXCLUDE |
| Iterative expansion | AutoLink (2025) | Start with anchors, expand via paths |
