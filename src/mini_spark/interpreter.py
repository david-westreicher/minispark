import time
from copy import deepcopy

from colorama import Fore, Style
from parsimonious.exceptions import ParseError
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings, KeyPressEvent

from mini_spark.dataframe import DataFrame
from mini_spark.plan import PhysicalPlan

from .execution import ThreadEngine
from .parser import SemanticError, parse_sql

kb = KeyBindings()


def format_duration(seconds: float) -> str:
    """Format seconds into a human-readable string."""
    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    if seconds < 60:  # noqa: PLR2004
        return f"{seconds:.2f} s"
    if seconds < 3600:  # noqa: PLR2004
        minutes, sec = divmod(seconds, 60)
        return f"{int(minutes)} min {sec:.2f} s"
    hours, rem = divmod(seconds, 3600)
    minutes, sec = divmod(rem, 60)
    return f"{int(hours)} h {int(minutes)} min {sec:.2f} s"


@kb.add("enter")
def _(event: KeyPressEvent) -> None:
    if event.current_buffer.text.strip()[-1:] == ";":
        event.current_buffer.validate_and_handle()
    else:
        event.current_buffer.insert_text("\n")


# History file
history = FileHistory("sql_history.txt")
session: PromptSession[str] = PromptSession(
    history=history,
    auto_suggest=AutoSuggestFromHistory(),
    multiline=True,
    key_bindings=kb,
)

BEGIN_PROMPT = r"""
         _      _                   __
  __ _  (_)__  (_)__ ___  ___ _____/ /__
 /  ' \/ / _ \/ (_-</ _ \/ _ `/ __/  '_/
/_/_/_/_/_//_/_/___/ .__/\_,_/_/ /_/\_\
                  /_/              v0.1
"""

print(Fore.BLUE + Style.BRIGHT + BEGIN_PROMPT + Style.RESET_ALL)  # noqa: T201


def explain_plan(df: DataFrame) -> None:
    plan = deepcopy(df.task)
    print(Style.DIM, end="")  # noqa: T201
    print("#### Logical Plan:")  # noqa: T201
    plan.explain()
    print()  # noqa: T201
    print("#### Physical Plan:")  # noqa: T201
    PhysicalPlan.generate_physical_plan(plan).explain()
    print(Style.RESET_ALL, end="")  # noqa: T201
    print()  # noqa: T201


while True:
    try:
        query = session.prompt("mini> ")

        if query.lower() in ("exit;", "quit;"):
            break

        try:
            df = parse_sql(query)
        except (ParseError, SemanticError) as e:
            print(Fore.RED + Style.BRIGHT + f"Parse error: {e}" + Style.RESET_ALL)  # noqa: T201
            continue
        with ThreadEngine() as engine:
            df.engine = engine
            try:
                explain_plan(df)

                print(Fore.GREEN, end="")  # noqa: T201
                start = time.perf_counter()
                df.show()
                end = time.perf_counter()
                print(Style.RESET_ALL, end="")  # noqa: T201

                print(Style.DIM + "Query time: " + format_duration(end - start) + Style.RESET_ALL)  # noqa: T201
            except Exception as e:  # noqa: BLE001
                print(Style.RESET_ALL)  # noqa: T201
                print(Fore.RED + Style.BRIGHT + f"Execution error: {e}" + Style.RESET_ALL)  # noqa: T201
            print(Style.RESET_ALL)  # noqa: T201

    except KeyboardInterrupt:
        continue
    except EOFError:
        break
print(Fore.BLUE + Style.BRIGHT + "Ciao!" + Style.RESET_ALL)  # noqa: T201
