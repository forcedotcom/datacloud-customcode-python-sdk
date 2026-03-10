# Schema Extraction & OpenAPI Spec Generation

Write a Python program to do the following:

**Input** is a Python file. Output is an OpenAPI schema file. The program will scan the Python file, find the function using the decorator `entry_func`, then extract the function schema, and generate an OpenAPI spec in the following examples. It will allow:

1) Function to be called via REST API

2) Function prototype in x-function field

3) Input/output parameters

4) Schema can be defined in two ways:
   - **Inline type annotations** on the function signature (e.g. `Dict[str, int]`)
   - **`@requestSchema` / `@responseSchema` decorators** that reference a class with typed fields (recommended for named properties)

5) If no schema information is available (bare `dict` without `@requestSchema`/`@responseSchema`), it would report errors

## Constraints

Each `@entry_func` function must follow these rules:

- **One parameter only**, named `request`, with a top-level `Dict` type (e.g. `dict`, `Dict`, `Dict[str, int]`, `Dict[str, List[str]]`)
- **Return type must be `Dict`** at the top level (e.g. `dict`, `Dict`, `Dict[str, int]`, `Dict[str, List[int]]`)
- **`Any` is not allowed** anywhere in the type annotations — all types must be fully specified
- The `Dict` may contain any supported nested types: `int`, `float`, `str`, `bool`, `List`, `Dict`, and combinations thereof
- **Schema must be defined** via one of:
  1. Parameterized `Dict` type annotations (e.g. `Dict[str, int]`) on the function signature, OR
  2. `@requestSchema` and `@responseSchema` decorators referencing typed classes
- **Bare `dict` without `@requestSchema`/`@responseSchema`** is an error — there is no way to extract schema information

Functions that violate these rules will produce a clear error message.

## Schema Definition via `@requestSchema` / `@responseSchema`

The `@requestSchema` and `@responseSchema` decorators define the request and response schemas using classes with typed fields. This is the **recommended** approach because it produces OpenAPI specs with named `properties` (rather than just `additionalProperties` from generic Dict types).

### Supported class types

The `@requestSchema` / `@responseSchema` decorators accept any class that has typed fields:

- **`TypedDict`** subclasses
- Plain classes with `__annotations__`

### Rules

- `@requestSchema` defines the request body schema. `@responseSchema` defines the response schema.
- Both decorators must appear **between** `@entry_func` and the function definition.
- When `@requestSchema` is present, the `request` parameter may use bare `dict` or `Dict` (the schema comes from the decorator).
- When `@responseSchema` is present, the return type may use bare `dict` or `Dict` (the schema comes from the decorator).
- If `@requestSchema` is absent, the `request` parameter **must** use a parameterized `Dict` type (e.g. `Dict[str, int]`).
- If `@responseSchema` is absent, the return type **must** use a parameterized `Dict` type.
- All field types in the referenced class must be fully specified (no `Any`).
- Supported field types: `int`, `float`, `str`, `bool`, `List`, `Dict`, `Optional`, and nested combinations.
- Fields annotated with `Optional[X]` are included in the OpenAPI `properties` but **omitted** from the `required` list.

## Sample Input — `@requestSchema` / `@responseSchema` with TypedDict

```python
from typing import TypedDict, List
from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema

class AddRequest(TypedDict):
    a: int
    b: int

class AddResponse(TypedDict):
    result: int

@entry_func
@requestSchema(AddRequest)
@responseSchema(AddResponse)
def add(request: dict) -> dict:
    """Add two integers."""
    a = request.get("a", 0)
    b = request.get("b", 0)
    return {"result": a + b}
```

## Expected Output — `@requestSchema` / `@responseSchema` with TypedDict

```yaml
openapi: 3.0.0
info:
  title: Add Service
  description: API generated from Python function schema
  version: "1.0.0"

paths:
  /add:
    post:
      summary: Add two integers
      description: Add two integers.
      operationId: add

      x-function:
        language: python
        namespace: myOrg
        package: myPackage
        name: addFn
        prototype: "add(request: dict) -> dict"
        requestSchema: "dict(a: int, b: int)"
        responseSchema: "dict(result: int)"
        description: Will perform add operation

      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                a:
                  type: integer
                b:
                  type: integer
              required:
                - a
                - b

      responses:
        "200":
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  result:
                    type: integer
                required:
                  - result
        "400":
          description: Invalid input
```

## Sample Input — `@requestSchema` / `@responseSchema` with nested types

```python
from typing import TypedDict, List, Dict
from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema, responseSchema

class AnalyzeRequest(TypedDict):
    dataset: str
    filters: Dict[str, List[str]]
    limit: int

class AnalyzeResponse(TypedDict):
    rows: List[Dict[str, int]]
    total: int

@entry_func
@requestSchema(AnalyzeRequest)
@responseSchema(AnalyzeResponse)
def analyze(request: dict) -> dict:
    """Analyze data with filters and return grouped results."""
    return {}
```

## Expected Output — `@requestSchema` / `@responseSchema` with nested types

```yaml
openapi: 3.0.0
info:
  title: Analyze Service
  description: API generated from Python function schema
  version: "1.0.0"

paths:
  /analyze:
    post:
      summary: Analyze data with filters and return grouped results
      description: Analyze data with filters and return grouped results.
      operationId: analyze

      x-function:
        language: python
        namespace: myOrg
        package: myPackage
        name: analyzeFn
        prototype: "analyze(request: dict) -> dict"
        requestSchema: "dict(dataset: str, filters: Dict[str, List[str]], limit: int)"
        responseSchema: "dict(rows: List[Dict[str, int]], total: int)"
        description: Will perform analyze operation

      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                dataset:
                  type: string
                filters:
                  type: object
                  additionalProperties:
                    type: array
                    items:
                      type: string
                limit:
                  type: integer
              required:
                - dataset
                - filters
                - limit

      responses:
        "200":
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  rows:
                    type: array
                    items:
                      type: object
                      additionalProperties:
                        type: integer
                  total:
                    type: integer
                required:
                  - rows
                  - total
        "400":
          description: Invalid input
```

## Sample Input — Inline typed Dict (no decorators)

```python
from typing import Dict, List
from datacustomcode.entry_func import entry_func

@entry_func
def analyze(request: Dict[str, Dict[str, int]]) -> Dict[str, List[int]]:
    """Analyze data with nested configuration and return grouped results."""
    return {}
```

## Expected Output — Inline typed Dict (no decorators)

```yaml
openapi: 3.0.0
info:
  title: Analyze Service
  description: API generated from Python function schema
  version: "1.0.0"

paths:
  /analyze:
    post:
      summary: Analyze data with nested configuration and return grouped results
      description: Analyze data with nested configuration and return grouped results.
      operationId: analyze

      x-function:
        language: python
        namespace: myOrg
        package: myPackage
        name: analyzeFn
        prototype: "analyze(request: Dict[str, Dict[str, int]]) -> Dict[str, List[int]]"
        description: Will perform analyze operation

      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              additionalProperties:
                type: object
                additionalProperties:
                  type: integer

      responses:
        "200":
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                additionalProperties:
                  type: array
                  items:
                    type: integer
        "400":
          description: Invalid input
```

## Sample Input — Mixed (`@requestSchema` only, inline return type)

```python
from typing import TypedDict, Dict, List
from datacustomcode.entry_func import entry_func
from datacustomcode.schema import requestSchema

class TransformRequest(TypedDict):
    values: List[int]
    scale: float

@entry_func
@requestSchema(TransformRequest)
def transform(request: dict) -> Dict[str, List[float]]:
    """Scale a list of values."""
    return {}
```

## Expected Output — Mixed (`@requestSchema` only, inline return type)

```yaml
openapi: 3.0.0
info:
  title: Transform Service
  description: API generated from Python function schema
  version: "1.0.0"

paths:
  /transform:
    post:
      summary: Scale a list of values
      description: Scale a list of values.
      operationId: transform

      x-function:
        language: python
        namespace: myOrg
        package: myPackage
        name: transformFn
        prototype: "transform(request: dict) -> Dict[str, List[float]]"
        requestSchema: "dict(values: List[int], scale: float)"
        description: Will perform transform operation

      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                values:
                  type: array
                  items:
                    type: integer
                scale:
                  type: number
              required:
                - values
                - scale

      responses:
        "200":
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                additionalProperties:
                  type: array
                  items:
                    type: number
        "400":
          description: Invalid input
```

## Error Cases

The following will all produce errors:

```python
# ERROR: multiple parameters (must be single "request" param)
@entry_func
def bad(request: dict, extra: int) -> dict: ...

# ERROR: wrong parameter name (must be "request")
@entry_func
def bad(data: dict) -> dict: ...

# ERROR: parameter type is not Dict
@entry_func
def bad(request: str) -> dict: ...

# ERROR: return type is not Dict
@entry_func
def bad(request: dict) -> int: ...

# ERROR: missing return type
@entry_func
def bad(request: dict): ...

# ERROR: Any is not allowed in type annotations
@entry_func
def bad(request: Dict[str, Any]) -> dict: ...

# ERROR: Any nested inside types is not allowed
@entry_func
def bad(request: Dict[str, List[Any]]) -> dict: ...

# ERROR: bare dict without @requestSchema — no schema info for request
@entry_func
def bad(request: dict) -> Dict[str, int]: ...

# ERROR: bare dict without @responseSchema — no schema info for response
@entry_func
def bad(request: Dict[str, int]) -> dict: ...

# ERROR: bare dict for both without any decorators — no schema info at all
@entry_func
def bad(request: dict) -> dict: ...

# ERROR: Any in @requestSchema class field
class BadRequest(TypedDict):
    data: Any

@entry_func
@requestSchema(BadRequest)
@responseSchema(SomeResponse)
def bad(request: dict) -> dict: ...

# ERROR: @requestSchema class has no typed fields
@entry_func
@requestSchema(dict)
@responseSchema(SomeResponse)
def bad(request: dict) -> dict: ...
```
