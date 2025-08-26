from typing import Callable, Dict

TASKS: Dict[str, Callable[[str], None]] = {}

def task(name: str):
    def deco(fn: Callable[[str], None]):
        TASKS[name] = fn
        return fn
    return deco

@task("echo")
def echo(payload: str):
    print(f"[task:echo] {payload}")