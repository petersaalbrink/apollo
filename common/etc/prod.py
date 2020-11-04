def prod(iterable, *, start=1):
    for i in iterable:
        start *= i
    return start
