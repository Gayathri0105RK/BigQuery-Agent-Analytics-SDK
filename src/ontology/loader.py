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

"""Loader and validator for v0 ontology YAML.

Implements the validation rules in §10 of ``docs/ontology/ontology.md``. Pydantic
covers shape (required fields, enum membership, unknown keys);
``_validate_ontology`` covers cross-element semantics (uniqueness,
inheritance cycles, key references, covariant narrowing, key-mode rules).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import yaml

from .models import Entity, Keys, Ontology, Relationship


# --------------------------------------------------------------------- #
# Public entry points                                                    #
# --------------------------------------------------------------------- #


def load_ontology(path: str | Path) -> Ontology:
  """Load and validate an ontology from a YAML file."""
  text = Path(path).read_text(encoding="utf-8")
  return load_ontology_from_string(text)


def load_ontology_from_string(yaml_string: str) -> Ontology:
  """Parse and validate an ontology from a YAML string.

  Raises:
      ValueError: On any §10 validation failure.
      pydantic.ValidationError: On shape failures (unknown keys, bad enums).
      yaml.YAMLError: On malformed YAML.
  """
  data = yaml.safe_load(yaml_string)
  if not isinstance(data, dict):
    raise ValueError("Ontology document must be a YAML mapping.")
  ontology = Ontology(**data)
  _validate_ontology(ontology)
  return ontology


# --------------------------------------------------------------------- #
# Validation                                                             #
# --------------------------------------------------------------------- #


def _validate_ontology(ont: Ontology) -> None:
  """Run §10 validation rules over a parsed ontology."""
  entity_map = _check_unique_names(ont.entities, "entity")
  rel_map = _check_unique_names(ont.relationships, "relationship")

  for ent in ont.entities:
    _check_property_names_unique(ent.properties, f"entity {ent.name!r}")
  for rel in ont.relationships:
    _check_property_names_unique(
        rel.properties, f"relationship {rel.name!r}"
    )

  _check_extends_targets(ont.entities, entity_map, "entity")
  _check_extends_targets(ont.relationships, rel_map, "relationship")
  _check_no_extends_cycles(ont.entities, "entity")
  _check_no_extends_cycles(ont.relationships, "relationship")

  _check_no_property_redeclaration(ont.entities, entity_map, "entity")
  _check_no_property_redeclaration(
      ont.relationships, rel_map, "relationship"
  )
  _check_no_key_redeclaration(ont.entities, entity_map, "entity")
  _check_no_key_redeclaration(ont.relationships, rel_map, "relationship")

  for ent in ont.entities:
    _check_entity_keys(ent, entity_map)
  for rel in ont.relationships:
    _check_relationship_keys(rel, rel_map)
    _check_relationship_endpoints(rel, entity_map)
    _check_covariant_narrowing(rel, rel_map, entity_map)


# --------------------------------------------------------------------- #
# Individual checks                                                      #
# --------------------------------------------------------------------- #


def _check_unique_names(items, kind: str) -> dict:
  """Rule 1: names unique within their kind."""
  out: dict = {}
  for item in items:
    if item.name in out:
      raise ValueError(f"Duplicate {kind} name: {item.name!r}")
    out[item.name] = item
  return out


def _check_property_names_unique(properties, owner: str) -> None:
  """Rule 2: property names unique within their owner."""
  seen: set[str] = set()
  for prop in properties:
    if prop.name in seen:
      raise ValueError(
          f"Duplicate property name {prop.name!r} on {owner}."
      )
    seen.add(prop.name)


def _check_extends_targets(items, item_map: dict, kind: str) -> None:
  """Rule 3: ``extends`` resolves to a same-kind declared element."""
  for item in items:
    if item.extends is not None and item.extends not in item_map:
      raise ValueError(
          f"{kind.capitalize()} {item.name!r} extends {item.extends!r}, "
          f"which is not a declared {kind}."
      )


def _check_no_extends_cycles(items, kind: str) -> None:
  """Rule 4: no cycles in ``extends`` chains."""
  parents = {i.name: i.extends for i in items}
  for start in parents:
    seen: set[str] = set()
    cur = start
    while cur is not None:
      if cur in seen:
        raise ValueError(f"Cycle in {kind} extends chain at {start!r}.")
      seen.add(cur)
      cur = parents.get(cur)


def _ancestors(name: str, item_map: dict) -> Iterable:
  """Yield ancestor items (excluding self) walking ``extends``."""
  cur = item_map[name].extends
  while cur is not None:
    parent = item_map[cur]
    yield parent
    cur = parent.extends


def _check_no_property_redeclaration(items, item_map: dict, kind: str) -> None:
  """Rule 5: redeclaring an inherited property by name is an error."""
  for item in items:
    if item.extends is None:
      continue
    inherited: set[str] = set()
    for ancestor in _ancestors(item.name, item_map):
      inherited.update(p.name for p in ancestor.properties)
    for prop in item.properties:
      if prop.name in inherited:
        raise ValueError(
            f"{kind.capitalize()} {item.name!r} redeclares inherited "
            f"property {prop.name!r}."
        )


def _check_no_key_redeclaration(items, item_map: dict, kind: str) -> None:
  """Rule 6: redeclaring inherited keys is an error."""
  for item in items:
    if item.extends is None:
      continue
    has_inherited_keys = any(
        _has_keys(a) for a in _ancestors(item.name, item_map)
    )
    if has_inherited_keys and _has_keys(item):
      raise ValueError(
          f"{kind.capitalize()} {item.name!r} redeclares inherited keys."
      )


def _has_keys(item) -> bool:
  keys = getattr(item, "keys", None)
  if keys is None:
    return False
  return bool(keys.primary or keys.additional or keys.alternate)


def _effective_properties(item, item_map: dict) -> dict:
  """All properties visible on ``item`` including inherited ones."""
  out: dict = {}
  for ancestor in reversed(list(_ancestors(item.name, item_map))):
    for p in ancestor.properties:
      out[p.name] = p
  for p in item.properties:
    out[p.name] = p
  return out


def _effective_keys(item, item_map: dict) -> Keys | None:
  """Resolve keys, walking up ``extends`` if not declared locally."""
  if _has_keys(item):
    return item.keys
  for ancestor in _ancestors(item.name, item_map):
    if _has_keys(ancestor):
      return ancestor.keys
  return None


def _check_key_columns_known(keys: Keys, props: dict, owner: str) -> None:
  """Rule 7: every key column references a declared property."""
  groups: list[list[str]] = []
  if keys.primary:
    groups.append(keys.primary)
  if keys.additional:
    groups.append(keys.additional)
  if keys.alternate:
    groups.extend(keys.alternate)
  for group in groups:
    for col in group:
      if col not in props:
        raise ValueError(
            f"{owner}: key column {col!r} is not a declared property."
        )


def _check_alternate_keys(keys: Keys, owner: str) -> None:
  """Rule 8: alternate keys are non-empty and non-duplicating."""
  if not keys.alternate:
    return
  if keys.primary is None:
    raise ValueError(
        f"{owner}: alternate keys require a primary key."
    )
  primary_set = frozenset(keys.primary)
  seen: set[frozenset] = {primary_set}
  for alt in keys.alternate:
    if not alt:
      raise ValueError(f"{owner}: alternate key must be non-empty.")
    if len(set(alt)) != len(alt):
      raise ValueError(
          f"{owner}: alternate key {alt!r} has duplicate columns."
      )
    sig = frozenset(alt)
    if sig in seen:
      raise ValueError(
          f"{owner}: alternate key {alt!r} duplicates another key."
      )
    seen.add(sig)


def _check_entity_keys(entity: Entity, entity_map: dict) -> None:
  """Rules 7, 8, 9, 11 for entities."""
  keys = _effective_keys(entity, entity_map)
  if keys is None or not keys.primary:
    raise ValueError(
        f"Entity {entity.name!r}: keys.primary is required."
    )
  if keys.additional is not None:
    raise ValueError(
        f"Entity {entity.name!r}: keys.additional is not allowed."
    )
  props = _effective_properties(entity, entity_map)
  _check_key_columns_known(keys, props, f"Entity {entity.name!r}")
  _check_alternate_keys(keys, f"Entity {entity.name!r}")


def _check_relationship_keys(rel: Relationship, rel_map: dict) -> None:
  """Rules 7, 8, 9 for relationships."""
  keys = _effective_keys(rel, rel_map)
  if keys is None:
    return  # no uniqueness constraint; multi-edges permitted.
  if keys.primary and keys.additional:
    raise ValueError(
        f"Relationship {rel.name!r}: primary and additional are "
        f"mutually exclusive."
    )
  if keys.additional is not None and keys.alternate:
    raise ValueError(
        f"Relationship {rel.name!r}: alternate keys require a primary key."
    )
  props = _effective_properties(rel, rel_map)
  _check_key_columns_known(keys, props, f"Relationship {rel.name!r}")
  _check_alternate_keys(keys, f"Relationship {rel.name!r}")


def _check_relationship_endpoints(
    rel: Relationship, entity_map: dict
) -> None:
  """Endpoints must reference declared entities."""
  if rel.from_ not in entity_map:
    raise ValueError(
        f"Relationship {rel.name!r}: from {rel.from_!r} is not a "
        f"declared entity."
    )
  if rel.to not in entity_map:
    raise ValueError(
        f"Relationship {rel.name!r}: to {rel.to!r} is not a declared entity."
    )


def _is_entity_subtype(child: str, parent: str, entity_map: dict) -> bool:
  """True if ``child`` equals ``parent`` or transitively extends it."""
  cur: str | None = child
  while cur is not None:
    if cur == parent:
      return True
    cur = entity_map[cur].extends
  return False


def _check_covariant_narrowing(
    rel: Relationship, rel_map: dict, entity_map: dict
) -> None:
  """Rule 10: child endpoints must equal or extend the parent's."""
  if rel.extends is None:
    return
  parent = rel_map[rel.extends]
  if not _is_entity_subtype(rel.from_, parent.from_, entity_map):
    raise ValueError(
        f"Relationship {rel.name!r}: from {rel.from_!r} does not "
        f"narrow parent from {parent.from_!r}."
    )
  if not _is_entity_subtype(rel.to, parent.to, entity_map):
    raise ValueError(
        f"Relationship {rel.name!r}: to {rel.to!r} does not narrow "
        f"parent to {parent.to!r}."
    )
