# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the canonical agent evaluation rubrics."""

from bigquery_agent_analytics.evaluation_rubrics import policy_compliance_metric
from bigquery_agent_analytics.evaluation_rubrics import response_usefulness_metric
from bigquery_agent_analytics.evaluation_rubrics import task_grounding_metric
from bigquery_agent_analytics.evaluation_rubrics import three_pillar_scorecard_metrics


def test_response_usefulness_matches_quality_report_canonical():
  """Verifies that the SDK usefulness rubric exactly matches the script version."""
  from scripts.quality_report import get_eval_metrics
  canonical_metrics = get_eval_metrics()
  canonical = next(m for m in canonical_metrics if m.name == "response_usefulness")
  rubric = response_usefulness_metric()
  assert rubric.model_dump() == canonical.model_dump()


def test_task_grounding_matches_quality_report_canonical():
  """Verifies that the SDK grounding rubric exactly matches the script version."""
  from scripts.quality_report import get_eval_metrics
  canonical_metrics = get_eval_metrics()
  canonical = next(m for m in canonical_metrics if m.name == "task_grounding")
  rubric = task_grounding_metric()
  assert rubric.model_dump() == canonical.model_dump()


def test_policy_compliance_categories():
  """Verifies the new GRC pillar has correct categories for Phase 1."""
  metric = policy_compliance_metric()
  assert metric.name == "policy_compliance"
  category_names = {c.name for c in metric.categories}
  assert category_names == {"compliant", "violation"}
  assert all(c.definition for c in metric.categories)


def test_three_pillar_bundle_content():
  """Verifies the convenience bundle returns all three pillars in order."""
  metrics = three_pillar_scorecard_metrics()
  assert len(metrics) == 3
  assert metrics[0].name == "response_usefulness"
  assert metrics[1].name == "task_grounding"
  assert metrics[2].name == "policy_compliance"


def test_quality_report_uses_canonical_rubrics():
  """Integration verification protecting against template definition drift."""
  from scripts.quality_report import get_eval_metrics
  metrics = get_eval_metrics()
  assert metrics[0].model_dump() == response_usefulness_metric().model_dump()
  assert metrics[1].model_dump() == task_grounding_metric().model_dump()