import functools


def deep_get(nesting, default, *keys):
    try:
        return functools.reduce(lambda d, k: d[k], keys, nesting)
    except (KeyError, TypeError) as e:
        return default


def ideep_get(nesting, *keys):
    return deep_get(nesting, None, *keys)
