# This script extracts all ```python fenced code blocks from README.md
# and turns them into pytest-compatible test functions.
#
# It replaces the need for other packages that already do this but impose
# dependency constraints.
#
# Usage:
#   $ python extract_readme_tests.py
#   $ pytest tests/test_readme_blocks.py
#
# Result:
#   Each code block becomes an isolated `test_readme_block_{i}` function.


import re
from pathlib import Path

README = Path("README.md")
TEST_FILE = Path("tests/test_readme_blocks.py")

def extract_python_blocks(text: str) -> list[str]:
    """Extracts ```python ... ``` blocks."""
    pattern = re.compile(r"```python\n(.*?)```", re.DOTALL)
    return pattern.findall(text)

def strip_non_ascii(s: str) -> str:
    """Removes all non-ASCII characters from a string."""
    return s.encode("ascii", errors="ignore").decode("ascii")

def wrap_as_test_fn(code: str, idx: int) -> str:
    """Wrap each block in a pytest-style test function."""
    clean_code = strip_non_ascii(code.strip())
    indented = "\n".join("    " + line for line in clean_code.splitlines())
    return f"def test_readme_block_{idx}():\n{indented or '    pass'}\n"

def main():
    content = README.read_text(encoding="utf-8")
    blocks = extract_python_blocks(content)
    test_fns = [wrap_as_test_fn(code, i) for i, code in enumerate(blocks)]
    header = "# Auto-generated from README.md\nimport pytest\n\n"
    TEST_FILE.write_text(header + "\n\n".join(test_fns) + "\n", encoding="utf-8")
    print(f"Wrote {len(test_fns)} test(s) to {TEST_FILE}")

if __name__ == "__main__":
    main()
