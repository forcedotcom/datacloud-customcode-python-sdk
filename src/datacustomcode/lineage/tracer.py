# Copyright (c) 2025, Salesforce, Inc.
# SPDX-License-Identifier: Apache-2
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
"""Column-level lineage tracing over a DataFrame's analyzed logical plan.

Every column in a Spark plan is an attribute with a unique ``exprId``. Walking the
analyzed plan bottom-up, each attribute is resolved to the set of *leaf columns*
it is computed from, where a leaf column is ``(leaf relation identity, position)``.

Leaf identity (rather than ``exprId``) is what makes self-joins traceable: Spark
re-aliases one side of a self-join to fresh ``exprId`` values, but both sides
still sit on the same underlying RDD / local data.

Only the columns that feed a column's *value* are sources. Filter predicates,
join conditions and sort keys gate or order rows and are deliberately ignored.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    Hashable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as PySparkDataFrame

LeafColumn = Tuple[Hashable, int]
"""``(leaf relation identity, column position)``."""

UNTRACEABLE_SOURCE = "untraceable source"
AMBIGUOUS_SOURCE = "ambiguous source"

_OUTPUT_SEPARATOR = "\u0001"

# Expressions that hide the columns they read in a nested plan the JSON does not
# carry, so their inputs cannot be traced
_OPAQUE_EXPRESSIONS = frozenset(
    {"ScalarSubquery", "Exists", "ListQuery", "InSubquery", "LateralSubquery"}
)

# User-defined functions: their arguments are traced, but the body can read data
# the plan does not show (a captured lookup, a broadcast, a file)
_UDF_EXPRESSIONS = frozenset(
    {
        "PythonUDF",
        "PythonUDAF",
        "PythonUDTF",
        "ScalaUDF",
        "ScalaUDAF",
        "ScalaAggregator",
    }
)


class LineageTraceError(Exception):
    """Raised when a DataFrame's plan cannot be traced at all."""


@dataclass(frozen=True)
class ColumnTrace:
    """Lineage of one output column.

    Attributes:
        name: Output column name.
        leaves: Leaf columns the value is computed from. Empty for literals.
        opaque_reason: Set when the plan hides this column's inputs (e.g. a
            ``mapInPandas`` or subquery); ``leaves`` is then incomplete.
        direct: The column is its single leaf column passed through unchanged
            (possibly renamed), not computed from it.
        udf: The value is computed by a user-defined function, which may read
            more than ``leaves``.
    """

    name: str
    leaves: FrozenSet[LeafColumn] = field(default_factory=frozenset)
    opaque_reason: Optional[str] = None
    direct: bool = False
    udf: bool = False


@dataclass(frozen=True)
class _AttrTrace:
    leaves: FrozenSet[LeafColumn]
    opaque_reason: Optional[str] = None
    direct: bool = False
    udf: bool = False

    def merge(self, other: _AttrTrace) -> _AttrTrace:
        # A merge combines inputs, so the result is never a direct pass-through
        return _AttrTrace(
            self.leaves | other.leaves,
            self.opaque_reason or other.opaque_reason,
            udf=self.udf or other.udf,
        )


_EMPTY = _AttrTrace(frozenset())


@dataclass(frozen=True)
class TraceResult:
    """Lineage of one output column in terms of registered source fields.

    Attributes:
        sources: ``"Object.field"`` names the value is computed from.
        reason: Set when the column cannot be fully traced; ``sources`` must
            then not be used.
        udf: The value is computed by a user-defined function, so ``sources``
            may be incomplete and needs review.
    """

    sources: FrozenSet[str] = field(default_factory=frozenset)
    reason: Optional[str] = None
    udf: bool = False

    @property
    def unresolved(self) -> bool:
        return self.reason is not None


class SourceRegistry:
    """Maps leaf columns of DataFrames read from Data Cloud to their fields.

    Only direct columns name a field: a column a reader computes (e.g.
    ``upper(name).alias("normalized")``) does not claim the leaf it reads. A leaf
    claimed by two different fields (two direct columns over it, or two objects
    read from the same relation) is ambiguous, and so is anything traced to it.
    """

    def __init__(self) -> None:
        self._fields: Dict[LeafColumn, str] = {}
        self._ambiguous: Set[LeafColumn] = set()
        self.objects: List[str] = []

    def register(self, df: PySparkDataFrame, object_name: str) -> None:
        """Record that the direct columns of ``df`` are fields of ``object_name``."""
        for column in trace_columns(df):
            if not column.direct:
                continue
            (leaf,) = column.leaves
            field_name = f"{object_name}.{column.name}"
            if self._fields.setdefault(leaf, field_name) != field_name:
                self._ambiguous.add(leaf)
        if object_name not in self.objects:
            self.objects.append(object_name)

    def trace(self, df: PySparkDataFrame) -> Dict[str, TraceResult]:
        """Resolve each column of ``df`` to the registered fields it reads."""
        results = {}
        for column in trace_columns(df):
            if column.opaque_reason is not None:
                results[column.name] = TraceResult(reason=column.opaque_reason)
            elif any(leaf in self._ambiguous for leaf in column.leaves):
                results[column.name] = TraceResult(reason=AMBIGUOUS_SOURCE)
            elif any(leaf not in self._fields for leaf in column.leaves):
                results[column.name] = TraceResult(reason=UNTRACEABLE_SOURCE)
            else:
                results[column.name] = TraceResult(
                    sources=frozenset(self._fields[leaf] for leaf in column.leaves),
                    udf=column.udf,
                )
        return results


def trace_columns(df: PySparkDataFrame) -> List[ColumnTrace]:
    """Trace every output column of ``df`` back to its leaf columns.

    Args:
        df: A classic (non Spark Connect) PySpark DataFrame.

    Returns:
        One :class:`ColumnTrace` per column of ``df``, in column order.

    Raises:
        LineageTraceError: If the plan is not accessible or is malformed.
    """
    jdf = getattr(df, "_jdf", None)
    if jdf is None:
        raise LineageTraceError(
            "Lineage tracing requires a classic PySpark DataFrame; Spark Connect "
            "DataFrames are not supported."
        )
    jvm = df.sparkSession._jvm
    jplan = jdf.queryExecution().analyzed()
    nodes = json.loads(jplan.toJSON())
    jnodes = _jvm_preorder(jplan)
    if len(nodes) != len(jnodes):
        raise LineageTraceError(
            f"Plan JSON has {len(nodes)} nodes but the plan tree has {len(jnodes)}"
        )

    children = _children_from_preorder(nodes)
    outputs = [_parse_output(jnode) for jnode in jnodes]
    traces: Dict[int, _AttrTrace] = {}

    # In pre-order every child comes after its parent, so walking backwards
    # resolves all children before the node that consumes them
    for index in range(len(nodes) - 1, -1, -1):
        _trace_node(
            nodes[index],
            jnodes[index],
            outputs[index],
            [outputs[c] for c in children[index]],
            traces,
            jvm,
        )

    root_ids = outputs[0]
    if len(root_ids) != len(df.columns):
        raise LineageTraceError(
            f"Plan output has {len(root_ids)} columns but the DataFrame has "
            f"{len(df.columns)}"
        )
    columns = []
    for name, expr_id in zip(df.columns, root_ids, strict=False):
        trace = traces.get(expr_id, _EMPTY)
        columns.append(
            ColumnTrace(
                name, trace.leaves, trace.opaque_reason, trace.direct, trace.udf
            )
        )
    return columns


def _trace_node(
    node: dict,
    jnode: Any,
    output: List[int],
    child_outputs: List[List[int]],
    traces: Dict[int, _AttrTrace],
    jvm: Any,
) -> None:
    node_class = node["class"].rsplit(".", 1)[-1]

    if not child_outputs:
        key = _leaf_key(jnode, node_class, jvm)
        for position, expr_id in enumerate(output):
            traces[expr_id] = _AttrTrace(frozenset({(key, position)}), direct=True)
        return

    if node_class == "Union":
        # Union reuses child 0's exprIds; merge every child positionally into them
        for position, expr_id in enumerate(output):
            merged = _EMPTY
            for child in child_outputs:
                if position < len(child):
                    merged = merged.merge(traces.get(child[position], _EMPTY))
            traces[expr_id] = merged
    elif node_class == "Expand":
        for position, expr_id in enumerate(output):
            merged = _EMPTY
            for projection in node.get("projections", []):
                if position < len(projection):
                    merged = merged.merge(
                        _trace_expression(projection[position], traces)
                    )
            traces[expr_id] = merged
    elif node_class == "Generate":
        generated = [
            _expr_id(attr)
            for attr_list in node.get("generatorOutput", [])
            for attr in attr_list
        ]
        per_output = _trace_generator(node.get("generator", []), len(generated), traces)
        for expr_id, trace in zip(generated, per_output, strict=True):
            traces[expr_id] = trace
    else:
        for expression in _expression_trees(node):
            _trace_aliases(expression, traces)

    for expr_id in output:
        if expr_id not in traces:
            traces[expr_id] = _AttrTrace(
                frozenset(), f"columns produced by {node_class} cannot be traced"
            )


def _trace_aliases(expression: List[dict], traces: Dict[int, _AttrTrace]) -> None:
    """Record every Alias in a flattened expression tree as a new attribute."""
    for index, expr_node in enumerate(expression):
        if expr_node["class"].endswith(".Alias"):
            child = expression[index + 1 : _subtree_end(expression, index)]
            if (
                len(child) == 1
                and child[0]["class"].endswith(".AttributeReference")
                and _expr_id(child[0]) in traces
            ):
                # A rename keeps the column a direct pass-through
                traces[_expr_id(expr_node)] = traces[_expr_id(child[0])]
            else:
                traces[_expr_id(expr_node)] = _trace_expression(child, traces)


def _trace_generator(
    generator: List[dict], output_count: int, traces: Dict[int, _AttrTrace]
) -> List[_AttrTrace]:
    """Trace each output of a Generate node's generator expression.

    Outputs are traced individually where the generator's shape says which
    input feeds which output. Otherwise they share the input's trace only when
    that input is a source column passed through unchanged: a computed input
    (e.g. ``map('key', name)``) can mix literals and columns, and the shared
    trace would attribute the literal outputs to the columns.
    """
    whole = _trace_expression(generator, traces)
    if output_count <= 1 or whole.opaque_reason is not None or not whole.leaves:
        return [whole] * output_count
    generator_class = _class_name(generator[0])
    children = _child_subtrees(generator)
    per_output: Optional[List[_AttrTrace]] = None
    if generator_class == "Stack":
        # stack(n, e0, e1, ...) puts e_j in output j % output_count
        per_output = [_EMPTY] * output_count
        for position, arg in enumerate(children[1:]):
            slot = position % output_count
            per_output[slot] = per_output[slot].merge(_trace_expression(arg, traces))
    elif generator_class == "JsonTuple":
        # Every output is a value extracted from the one JSON input
        per_output = [whole] * output_count
    elif generator_class in ("Explode", "PosExplode", "Inline"):
        with_position = generator_class == "PosExplode"
        fields = _trace_collection(
            children[0],
            output_count - with_position,
            generator_class == "Inline",
            traces,
        )
        if fields is not None:
            per_output = [whole] * with_position + fields
    if per_output is None:
        reason = f"columns produced by {generator_class} cannot be traced"
        return [_AttrTrace(frozenset(), reason)] * output_count
    return per_output


def _trace_collection(
    collection: List[dict],
    field_count: int,
    of_structs: bool,
    traces: Dict[int, _AttrTrace],
) -> Optional[List[_AttrTrace]]:
    """Trace the fields a generator unpacks from each element of a collection.

    The fields are a map's key and value, or a struct's fields (``of_structs``).
    Returns ``None`` when it is not known which input feeds which field.
    """
    if _is_direct(collection, traces):
        return [_trace_expression(collection, traces)] * field_count
    root = _class_name(collection[0])
    if root == "CreateMap" and not of_structs and field_count == 2:
        # map(k0, v0, k1, v1, ...)
        fields = [_EMPTY, _EMPTY]
        for position, entry in enumerate(_child_subtrees(collection)):
            fields[position % 2] = fields[position % 2].merge(
                _trace_expression(entry, traces)
            )
        return fields
    if root == "CreateArray" and of_structs:
        fields = [_EMPTY] * field_count
        for element in _child_subtrees(collection):
            if _is_direct(element, traces):
                element_fields = [_trace_expression(element, traces)] * field_count
            elif _class_name(element[0]) == "CreateNamedStruct":
                # named_struct(name0, value0, name1, value1, ...)
                element_fields = [
                    _trace_expression(value, traces)
                    for value in _child_subtrees(element)[1::2]
                ]
            else:
                return None
            fields = [f.merge(e) for f, e in zip(fields, element_fields, strict=True)]
        return fields
    return None


def _is_direct(expression: List[dict], traces: Dict[int, _AttrTrace]) -> bool:
    """Whether a flattened expression is a source column passed through as-is."""
    return (
        len(expression) == 1
        and _class_name(expression[0]) == "AttributeReference"
        and traces.get(_expr_id(expression[0]), _EMPTY).direct
    )


def _class_name(expr_node: dict) -> str:
    return str(expr_node["class"]).rsplit(".", 1)[-1]


def _child_subtrees(expression: List[dict]) -> List[List[dict]]:
    """The flattened subtrees of the root's children, in order."""
    children = []
    index = 1
    for _ in range(expression[0]["num-children"]):
        end = _subtree_end(expression, index)
        children.append(expression[index:end])
        index = end
    return children


def _trace_expression(
    expression: List[dict], traces: Dict[int, _AttrTrace]
) -> _AttrTrace:
    """Union the traces of every attribute an expression (sub)tree references."""
    result = _EMPTY
    for expr_node in expression:
        expr_class = expr_node["class"].rsplit(".", 1)[-1]
        if expr_class in _OPAQUE_EXPRESSIONS:
            result = result.merge(
                _AttrTrace(frozenset(), f"{expr_class} inputs cannot be traced")
            )
        elif expr_class in _UDF_EXPRESSIONS:
            result = result.merge(_AttrTrace(frozenset(), udf=True))
        elif expr_class == "AttributeReference":
            expr_id = _expr_id(expr_node)
            if expr_id in traces:
                result = result.merge(traces[expr_id])
            else:
                result = result.merge(
                    _AttrTrace(frozenset(), f"unresolved attribute {expr_id}")
                )
    return result


def _expression_trees(value: Any) -> Iterator[List[dict]]:
    """Yield every flattened expression tree held in a plan node's fields."""
    if isinstance(value, dict):
        for key, field_value in value.items():
            if key not in ("class", "num-children"):
                yield from _expression_trees(field_value)
    elif isinstance(value, list) and value:
        if all(isinstance(item, dict) and "num-children" in item for item in value):
            yield value
        else:
            for item in value:
                if isinstance(item, list):
                    yield from _expression_trees(item)


def _subtree_end(nodes: List[dict], start: int) -> int:
    """Index one past the pre-order subtree rooted at ``start``."""
    pending = 1
    index = start
    while pending:
        pending += nodes[index]["num-children"] - 1
        index += 1
    return index


def _children_from_preorder(nodes: List[dict]) -> List[List[int]]:
    children: List[List[int]] = [[] for _ in nodes]
    open_parents: List[List[int]] = []  # [parent index, children still expected]
    for index, node in enumerate(nodes):
        if open_parents:
            parent = open_parents[-1]
            children[parent[0]].append(index)
            parent[1] -= 1
            if parent[1] == 0:
                open_parents.pop()
        if node["num-children"] > 0:
            open_parents.append([index, node["num-children"]])
    return children


def _jvm_preorder(jplan: Any) -> List[Any]:
    result = []
    stack = [jplan]
    while stack:
        jnode = stack.pop()
        result.append(jnode)
        jchildren = jnode.children()
        stack.extend(jchildren.apply(i) for i in reversed(range(jchildren.size())))
    return result


def _parse_output(jnode: Any) -> List[int]:
    """ExprIds of a plan node's output attributes, in order.

    One JVM round trip per node: attributes render as ``name#<id><suffix>``.
    """
    count = jnode.output().size()
    if count == 0:
        return []
    rendered = jnode.output().mkString(_OUTPUT_SEPARATOR).split(_OUTPUT_SEPARATOR)
    if len(rendered) != count:
        raise LineageTraceError("Could not parse plan output attributes")
    expr_ids = []
    for attr in rendered:
        digits = attr.rsplit("#", 1)[-1]
        end = 0
        while end < len(digits) and digits[end].isdigit():
            end += 1
        if end == 0:
            raise LineageTraceError(f"Could not parse plan attribute {attr!r}")
        expr_ids.append(int(digits[:end]))
    return expr_ids


def _expr_id(expr_node: dict) -> int:
    return int(expr_node["exprId"]["id"])


def _leaf_key(jnode: Any, node_class: str, jvm: Any) -> Hashable:
    identity = jvm.java.lang.System.identityHashCode
    if node_class == "LogicalRDD":
        return ("rdd", jnode.rdd().id())
    if node_class == "LocalRelation":
        return ("local", identity(jnode.data()))
    if node_class == "LogicalRelation":
        return ("relation", identity(jnode.relation()))
    if node_class == "DataSourceV2Relation":
        return ("v2", identity(jnode.table()))
    return ("node", identity(jnode))
