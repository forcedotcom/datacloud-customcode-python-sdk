from dataclasses import dataclass
from typing import List, Callable, Any


@dataclass
class Rule:
    id: str
    message: str
    expression: Callable[[Any], bool]  # Python function that evaluates the CEL expression


class ValidationViolation:
    """Represents a single validation violation"""
    def __init__(self, rule_id: str, message: str, field_path: str = ""):
        self.rule_id = rule_id
        self.message = message
        self.field_path = field_path

    def __str__(self):
        if self.field_path:
            return f"[{self.rule_id}] {self.field_path}: {self.message}"
        return f"[{self.rule_id}] {self.message}"



class ValidationResult:
    """Collection of validation violations"""
    def __init__(self, violations: List[ValidationViolation] = None):
        self.violations = violations or []

    def is_valid(self) -> bool:
        return len(self.violations) == 0

    def __bool__(self):
        return not self.is_valid()  # True if there are violations

    def __str__(self):
        if self.is_valid():
            return "No violations"
        return "\n".join(str(v) for v in self.violations)


class Validator:
    def __init__(self):
        self.rules: List[Rule] = []

    def add_rule(self, id: str, message: str, expression: Callable[[Any], bool]) -> 'CELValidator':
        """
        Args:
            id: Rule identifier (e.g., "request.prompt_limit")
            message: Error message if rule fails
            expression: Lambda that takes 'this' (the message) and returns bool
                       True = passes validation
                       False = fails validation

        Returns:
            self (for chaining)
        """
        self.rules.append(Rule(id=id, message=message, expression=expression))
        return self

    def validate(self, message: Any) -> ValidationResult:
        """

        Validate a message against all registered rules.

        Args:
            message: The message to validate (e.g., GenerateTextRequest)

        Returns:
            ValidationResult with any violations found
        """
        violations = []

        for rule in self.rules:
            try:
                # Evaluate the CEL expression (Python lambda)
                # 'this' in CEL becomes the message object in Python
                if not rule.expression(message):
                    violations.append(
                        ValidationViolation(
                            rule_id=rule.id,
                            message=rule.message
                        )
                    )
            except Exception as e:
                # If expression evaluation fails, treat as validation error
                violations.append(
                    ValidationViolation(
                        rule_id=rule.id,
                        message=f"{rule.message} (evaluation error: {e})"
                    )
                )

        return ValidationResult(violations)

