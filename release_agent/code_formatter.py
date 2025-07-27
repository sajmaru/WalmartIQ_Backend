# release_agent/code_formatter.py
"""
Advanced code formatting and validation for LLM-generated Python code.

This module provides robust validation, formatting, and fixing capabilities
for dynamically generated Python code to ensure it's syntactically correct
and properly formatted before execution.
"""

import ast
import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CodeFormatter:
    """Advanced code formatter and validator for LLM-generated code."""

    def __init__(self):
        self.required_imports = {
            "json",
            "networkx as nx",
            "pandas as pd",
            "numpy as np",
            "datetime",
        }

    def format_and_validate(self, code: str) -> Tuple[str, bool, List[str]]:
        """
        Format and validate generated code using multiple strategies.

        Args:
            code: Raw generated code string

        Returns:
            Tuple of (formatted_code, is_valid, error_messages)
        """
        errors = []

        # Step 1: Basic cleanup and preprocessing
        cleaned_code = self._preprocess_code(code)

        # Step 2: Fix common LLM generation issues
        fixed_code = self._fix_common_llm_issues(cleaned_code)

        # Step 3: Validate syntax
        is_valid, syntax_errors = self._validate_syntax(fixed_code)
        if not is_valid:
            errors.extend(syntax_errors)
            # Try advanced fixing
            fixed_code = self._advanced_syntax_fix(fixed_code, syntax_errors)
            is_valid, remaining_errors = self._validate_syntax(fixed_code)
            if not is_valid:
                errors.extend(remaining_errors)

        # Step 4: Format with black if valid
        if is_valid:
            formatted_code = self._format_with_black(fixed_code)
            if formatted_code:
                fixed_code = formatted_code

        # Step 5: Final validation
        final_valid, final_errors = self._validate_syntax(fixed_code)
        if not final_valid:
            errors.extend(final_errors)

        return fixed_code, final_valid, errors

    def _preprocess_code(self, code: str) -> str:
        """Clean and preprocess raw generated code."""
        # Remove markdown code blocks
        code = re.sub(r"^```python\s*\n", "", code, flags=re.MULTILINE)
        code = re.sub(r"\n```\s*$", "", code, flags=re.MULTILINE)

        # Remove any leading/trailing explanatory text
        lines = code.split("\n")
        start_idx = 0
        end_idx = len(lines)

        # Find first actual code line
        for i, line in enumerate(lines):
            stripped = line.strip()
            if (
                stripped.startswith(
                    ("import ", "from ", "#", "def ", "class ", "try:", "if ")
                )
                or "results = {" in stripped
                or stripped.startswith(("results[", "print("))
            ):
                start_idx = i
                break

        # Find last actual code line
        for i in range(len(lines) - 1, -1, -1):
            stripped = lines[i].strip()
            if (
                stripped
                and not stripped.startswith(("Note:", "Explanation:", "This code"))
                and not stripped.endswith((".", "!", "?"))
            ):
                end_idx = i + 1
                break

        cleaned_lines = lines[start_idx:end_idx]
        return "\n".join(cleaned_lines)

    def _fix_common_llm_issues(self, code: str) -> str:
        """Fix common issues in LLM-generated code."""
        lines = code.split("\n")
        fixed_lines = []

        in_try_block = False
        try_indent = 0

        for i, line in enumerate(lines):
            stripped = line.strip()
            current_indent = len(line) - len(line.lstrip())

            # Track try blocks
            if stripped.startswith("try:"):
                in_try_block = True
                try_indent = current_indent
                fixed_lines.append(line)
                continue

            # If we're in a try block and hit unindented import, close the try block
            if (
                in_try_block
                and stripped.startswith("import ")
                and current_indent <= try_indent
            ):
                # Add except block before the import
                except_line = " " * (try_indent + 4) + "pass"
                fixed_lines.append(" " * try_indent + "except Exception as e:")
                fixed_lines.append(except_line)
                in_try_block = False

            # Fix f-string issues (common in generated code)
            if 'f"' in line and "\\n" in line:
                line = line.replace("\\n", "\n")

            # Ensure proper indentation for results dictionary
            if "results = {" in stripped:
                fixed_lines.append(line)
                continue

            # Fix common quote issues
            if "'" in stripped and '"' in stripped:
                # Standardize on double quotes
                line = re.sub(r"(?<!\\)'", '"', line)

            fixed_lines.append(line)

        # Ensure code ends with print statement
        if fixed_lines:
            last_line = fixed_lines[-1].strip()
            if last_line == "results":
                fixed_lines[-1] = "print(json.dumps(results))"
            elif not last_line.startswith("print(json.dumps(results)"):
                fixed_lines.append("print(json.dumps(results))")

        return "\n".join(fixed_lines)

    def _validate_syntax(self, code: str) -> Tuple[bool, List[str]]:
        """Validate Python syntax and return detailed errors."""
        try:
            ast.parse(code)
            return True, []
        except SyntaxError as e:
            error_msg = f"Syntax error at line {e.lineno}: {e.msg}"
            if e.text:
                error_msg += f" in '{e.text.strip()}'"
            return False, [error_msg]
        except Exception as e:
            return False, [f"Parse error: {str(e)}"]

    def _advanced_syntax_fix(self, code: str, errors: List[str]) -> str:
        """Apply advanced syntax fixes based on error analysis."""
        lines = code.split("\n")

        # Fix incomplete try blocks
        if any("expected" in error and "except" in error for error in errors):
            lines = self._fix_incomplete_try_blocks(lines)

        # Fix indentation issues
        if any("indentation" in error.lower() for error in errors):
            lines = self._fix_indentation_issues(lines)

        # Fix quote issues
        if any("quote" in error.lower() for error in errors):
            lines = self._fix_quote_issues(lines)

        return "\n".join(lines)

    def _fix_incomplete_try_blocks(self, lines: List[str]) -> List[str]:
        """Fix incomplete try blocks by adding proper except clauses."""
        fixed_lines = []
        try_blocks = []

        for i, line in enumerate(lines):
            stripped = line.strip()
            current_indent = len(line) - len(line.lstrip())

            if stripped.startswith("try:"):
                try_blocks.append((i, current_indent))
                fixed_lines.append(line)
            elif stripped.startswith(("except", "finally")):
                # Remove completed try block
                if try_blocks:
                    try_blocks.pop()
                fixed_lines.append(line)
            elif try_blocks and current_indent <= try_blocks[-1][1]:
                # We've left the try block without except/finally
                try_indent = try_blocks[-1][1]
                fixed_lines.append(" " * try_indent + "except Exception as e:")
                fixed_lines.append(" " * (try_indent + 4) + "results['error'] = str(e)")
                try_blocks.pop()
                fixed_lines.append(line)
            else:
                fixed_lines.append(line)

        # Handle any remaining open try blocks
        for _, try_indent in try_blocks:
            fixed_lines.append(" " * try_indent + "except Exception as e:")
            fixed_lines.append(" " * (try_indent + 4) + "results['error'] = str(e)")

        return fixed_lines

    def _fix_indentation_issues(self, lines: List[str]) -> List[str]:
        """Fix common indentation issues."""
        fixed_lines = []
        expected_indent = 0

        for line in lines:
            if not line.strip():
                fixed_lines.append("")
                continue

            stripped = line.strip()
            current_indent = len(line) - len(line.lstrip())

            # Adjust indentation for control structures
            if stripped.endswith(":"):
                fixed_lines.append(" " * expected_indent + stripped)
                expected_indent += 4
            elif stripped.startswith(("except", "finally", "elif", "else")):
                expected_indent = max(0, expected_indent - 4)
                fixed_lines.append(" " * expected_indent + stripped)
                expected_indent += 4
            else:
                # Regular line
                if stripped.startswith(("import ", "from ")):
                    fixed_lines.append(stripped)  # Imports at top level
                else:
                    fixed_lines.append(" " * expected_indent + stripped)

        return fixed_lines

    def _fix_quote_issues(self, lines: List[str]) -> List[str]:
        """Fix quote-related syntax issues."""
        fixed_lines = []

        for line in lines:
            # Fix mixed quotes
            if "'" in line and '"' in line:
                # Find the dominant quote type
                single_quotes = line.count("'")
                double_quotes = line.count('"')

                if single_quotes > double_quotes:
                    # Use single quotes, escape doubles
                    line = line.replace('"', '\\"')
                else:
                    # Use double quotes, escape singles
                    line = line.replace("'", "\\'")

            fixed_lines.append(line)

        return fixed_lines

    def _format_with_black(self, code: str) -> Optional[str]:
        """Format code using black if available."""
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(code)
                temp_path = f.name

            # Run black on the temporary file
            result = subprocess.run(
                ["black", "--quiet", temp_path],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                with open(temp_path, "r") as f:
                    formatted_code = f.read()
                Path(temp_path).unlink()  # Clean up
                return formatted_code
            else:
                logger.warning(f"Black formatting failed: {result.stderr}")
                Path(temp_path).unlink()  # Clean up
                return None

        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            logger.warning(f"Could not format with black: {e}")
            return None

    def generate_fallback_code(self, query: str, target_files: List[str]) -> str:
        """Generate a simple fallback code template when all else fails."""
        return f"""
import json
import networkx as nx
from datetime import datetime

# Fallback code for query: {query}
results = {{
    'data': [],
    'metadata': {{
        'query': "{query}",
        'target_files': {target_files},
        'status': 'fallback_mode',
        'timestamp': datetime.now().isoformat()
    }},
    'summary': {{
        'total_records': 0,
        'message': 'Fallback mode - original query code had syntax errors'
    }},
    'error': 'Code generation failed, using fallback template'
}}

print(json.dumps(results))
"""
