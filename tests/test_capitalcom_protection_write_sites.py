"""
@pyne

Every venue write that changes a stop loss or a take profit is recorded with
the protection guard before it leaves. A write the guard does not expect comes
back from the venue looking like someone else's edit, and the bot quarantines
itself on a live account.

The rule is checked on the source: each call that issues such a write must sit
in a function that goes through a guard entry point, or in a transport helper
whose every caller does. A new write site outside them fails here.
"""
import ast
from pathlib import Path

import pynecore_capitalcom

__test_helper_SOURCES = Path(pynecore_capitalcom.__file__).parent

#: Names a function must mention to count as recording its write with the guard.
__test_helper_GUARD_ENTRY_POINTS = frozenset({'_protection_write', 'protection_write', '_own_write_in_flight', 'working_order_write'})

#: Functions that only carry the request; their callers hold the guard.
__test_helper_TRANSPORT_HELPERS = frozenset({'_put_bracket_with_notfound_retry'})


def __test_helper_is_protection_write(call: ast.Call) -> bool:
    """A PUT: the venue's only PUTs amend a position's or a working order's levels."""
    return any(kw.arg == 'method' and isinstance(kw.value, ast.Constant)
               and kw.value.value == 'put' for kw in call.keywords)


def __test_helper_functions(tree: ast.AST) -> list:
    return [node for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]


def __test_helper_innermost(functions: list, line: int):
    enclosing = [fn for fn in functions if fn.lineno <= line <= fn.end_lineno]
    return max(enclosing, key=lambda fn: fn.lineno) if enclosing else None


def __test_helper_names(function) -> set:
    names = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
    return names


def __test_every_protection_write_goes_through_the_guard__():
    trees = {path.name: ast.parse(path.read_text())
             for path in sorted(__test_helper_SOURCES.glob('*.py'))}
    functions = {name: __test_helper_functions(tree) for name, tree in trees.items()}
    sites = []
    for name, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and __test_helper_is_protection_write(node):
                sites.append((name, node.lineno,
                              __test_helper_innermost(functions[name], node.lineno)))
    assert len(sites) >= 7, "the write sites were not found; the rule checks nothing"

    unguarded = []
    for name, line, function in sites:
        if function is None:
            unguarded.append(f"{name}:{line} at module level")
            continue
        if function.name in __test_helper_TRANSPORT_HELPERS:
            callers = [fn for fns in functions.values() for fn in fns
                       if fn is not function and function.name in __test_helper_names(fn)]
            assert callers, f"{function.name} has no caller"
            checked = callers
        else:
            checked = [function]
        for fn in checked:
            if not __test_helper_names(fn) & __test_helper_GUARD_ENTRY_POINTS:
                unguarded.append(f"{name}:{line} in {fn.name}")
    assert not unguarded, (
        "a stop-loss / take-profit write leaves without the protection guard:\n"
        + "\n".join(unguarded))
