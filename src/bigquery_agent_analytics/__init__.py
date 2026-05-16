# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BigQuery Agent Analytics SDK.

This package provides the consumption-layer SDK for analyzing agent traces
stored in BigQuery.
"""

import logging

logger = logging.getLogger("bigquery_agent_analytics." + __name__)

__all__ = []

# --- Telemetry primitives ---
from ._telemetry import LabeledBigQueryClient
from ._telemetry import make_bq_client
from ._telemetry import with_sdk_labels

__all__.extend(["LabeledBigQueryClient", "make_bq_client", "with_sdk_labels"])

# --- SDK Client & Core ---
try:
  from .client import Client
  from .evaluators import CodeEvaluator, EvaluationReport, LLMAsJudge, SessionScore
  from .feedback import AnalysisConfig, DriftReport, QuestionDistribution
  from .formatter import format_output
  from .insights import InsightsConfig, InsightsReport, SessionFacet
  from .serialization import serialize
  from .trace import ContentPart, EventType, ObjectRef, Span, Trace, TraceFilter
  from .views import ViewManager

  __all__.extend([
      "Client", "Trace", "Span", "ContentPart", "EventType", "ObjectRef",
      "TraceFilter", "ViewManager", "CodeEvaluator", "LLMAsJudge",
      "EvaluationReport", "SessionScore", "DriftReport", "QuestionDistribution",
      "AnalysisConfig", "format_output", "InsightsReport", "InsightsConfig",
      "serialize", "SessionFacet"
  ])
except ImportError as e:
  logger.debug("Could not import SDK client components: %s", e)

# --- Categorical Evaluator & Views ---
try:
  from .categorical_evaluator import (
      CategoricalEvaluationConfig, CategoricalEvaluationReport,
      CategoricalMetricCategory, CategoricalMetricDefinition,
      CategoricalMetricResult, CategoricalSessionResult
  )
  from .categorical_views import CategoricalViewManager
  __all__.extend([
      "CategoricalEvaluationConfig", "CategoricalEvaluationReport",
      "CategoricalMetricCategory", "CategoricalMetricDefinition",
      "CategoricalMetricResult", "CategoricalSessionResult",
      "CategoricalViewManager"
  ])
except ImportError as e:
  logger.debug("Could not import categorical components: %s.", e)

# --- Ontology & Extraction (Latest Main) ---
try:
  from .ontology_models import ExtractedEdge, ExtractedGraph, ExtractedNode, ExtractedProperty
  from .ontology_orchestrator import build_ontology_graph, compile_lineage_gql, compile_showcase_gql
  from .ontology_property_graph import OntologyPropertyGraphCompiler, can_use_upstream_compiler
  from .ontology_schema_compiler import compile_extraction_prompt, compile_output_schema
  from .structured_extraction import run_structured_extractors, StructuredExtractor
  from .graph_validation import validate_extracted_graph
  from .extractor_compilation import compile_extractor

  __all__.extend([
      "ExtractedEdge", "ExtractedGraph", "ExtractedNode", "ExtractedProperty",
      "OntologyPropertyGraphCompiler", "can_use_upstream_compiler",
      "compile_extraction_prompt", "compile_output_schema",
      "build_ontology_graph", "compile_lineage_gql", "compile_showcase_gql",
      "run_structured_extractors", "StructuredExtractor",
      "validate_extracted_graph", "compile_extractor"
  ])
except ImportError as e:
  logger.debug("Could not import ontology/extraction components: %s.", e)

# --- Ontology Runtime Reader ---
try:
  from .ontology_runtime import (
      ConceptIndexLookup, EntityResolver, OntologyRuntime, ResolverCandidate
  )
  __all__.extend([
      "ConceptIndexLookup", "EntityResolver", "OntologyRuntime", "ResolverCandidate"
  ])
except ImportError as e:
  logger.debug("Could not import ontology runtime: %s.", e)

# --- Evaluation Rubrics (Phase 1) ---
try:
  from .evaluation_rubrics import (
      policy_compliance_metric, response_usefulness_metric,
      task_grounding_metric, three_pillar_scorecard_metrics
  )
  __all__.extend([
      "policy_compliance_metric", "response_usefulness_metric",
      "task_grounding_metric", "three_pillar_scorecard_metrics"
  ])
except ImportError as e:
  logger.debug("Could not import evaluation rubrics: %s.", e)
