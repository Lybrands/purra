"""Test-only import tripwire for source and installed SDK checks."""
import sys
from importlib.abc import MetaPathFinder

class DenyLangChain(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"langchain", "langchain_core", "langchain_community", "langsmith"}:
            raise AssertionError("LangChain import attempted: " + fullname)

sys.meta_path.insert(0, DenyLangChain())
